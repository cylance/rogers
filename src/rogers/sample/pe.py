""" Static analysis of PE files
"""
import pefile

from rogers.sample import Sample
import rogers.util as u

from rogers.logger import get_logger

log = get_logger(__name__)


class PE(Sample):

    def extract(self):
        """ Extract raw PE features
        Adopted from following sources:
            1) "Lens on the endpoint: Hunting for malicious software through endpoint data analysis", http://www.ccs.neu.edu/home/alina/papers/Endpoint.pdf
            2) "Survey on the Usage of Machine Learning Techniques for Malware Analysis", https://arxiv.org/abs/1710.08189
        :return:
        """
        # calculate and set sha256 in features
        self.features.sha256 = self.sha256
        with self.tempfile() as tmp_path:
            # match yara signatures
            self.add('static.signatures', self._yara(tmp_path))
            try:
                # attempt to extract pe file characteristics
                pe = pefile.PE(tmp_path)
            except pefile.PEFormatError:
                log.error("pe format error: %s", self.sha256)
            else:
                self.add('file_size', len(pe.__data__))
                self.add('header.image_size', int(pe.OPTIONAL_HEADER.SizeOfImage))
                self.add('header.epoch_timestamp', int(pe.FILE_HEADER.TimeDateStamp))
                self.add('header.machine', int(pe.FILE_HEADER.Machine))
                self.add('header.entrypoint', int(pe.OPTIONAL_HEADER.AddressOfEntryPoint))
                self.add('header.image_base', int(pe.OPTIONAL_HEADER.ImageBase))
                self.add('header.n_sections', int(pe.FILE_HEADER.NumberOfSections))
                self.add('header.char', int(pe.FILE_HEADER.Characteristics))
                self.add('header.major_link', int(pe.OPTIONAL_HEADER.MajorLinkerVersion))
                self.add('header.minor_link', int(pe.OPTIONAL_HEADER.MinorLinkerVersion))
                self.add('header.is_exe', (True if pe.is_exe() else False))
                self.add('header.is_driver', (True if pe.is_driver() else False))
                self.add('header.is_dll', (True if pe.is_dll() else False))
                self.add('header.code_size', int(pe.OPTIONAL_HEADER.SizeOfCode))
                self.add('header.initdata_size', int(pe.OPTIONAL_HEADER.SizeOfInitializedData))
                self.add('header.uninit_size', int(pe.OPTIONAL_HEADER.SizeOfUninitializedData))
                self.add('header.n_rva', int(pe.OPTIONAL_HEADER.NumberOfRvaAndSizes))

                # version info
                if hasattr(pe, 'VS_FIXEDFILEINFO'):
                    ms = pe.VS_FIXEDFILEINFO.ProductVersionMS
                    ls = pe.VS_FIXEDFILEINFO.ProductVersionLS
                    self.add('header.version_identifier', "%s.%s.%s.%s" % (u.hiword(ms), u.loword(ms), u.hiword(ls), u.loword(ls)))

                # sym exports
                if hasattr(pe, 'DIRECTORY_ENTRY_IMPORT'):
                    syms = set()
                    for entry in pe.DIRECTORY_ENTRY_IMPORT:
                        if entry.dll is not None:
                            for imp in entry.imports:
                                if imp.name is not None:
                                    syms.add("%s-%s" % (u.to_ascii(entry.dll), u.to_ascii(imp.name)))
                    self.add('header.import_syms', list(syms))

                # sym exports
                if hasattr(pe, 'DIRECTORY_ENTRY_EXPORT'):
                    syms = set()
                    for exp in pe.DIRECTORY_ENTRY_EXPORT.symbols:
                        if exp.name is not None:
                            syms.add(u.to_ascii(exp.name))
                    self.add('header.export_syms', list(syms))

                section_names = []
                section_entropy = {}
                section_raw_size = {}
                section_virtual_size = {}
                # section info
                for section in pe.sections:
                    if not section:
                        continue
                    sec_name = u.to_ascii(section.Name).replace('.', '')
                    section_names.append(sec_name)
                    section_entropy[sec_name] = section.get_entropy()
                    section_raw_size[sec_name] = int(section.SizeOfRawData)
                    section_virtual_size[sec_name] = int(section.Misc_VirtualSize)
                self.add('header.section_entropy', section_entropy)
                self.add('header.section_raw_size', section_raw_size)
                self.add('header.section_virtual_size', section_virtual_size)
                self.add('header.section_names', section_names)