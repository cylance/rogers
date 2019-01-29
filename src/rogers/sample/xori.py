from .pe import PE
from .. import config as c

import json
import os
import os.path
import subprocess

from ..logger import get_logger

log = get_logger(__name__)

# TODO: these are hard-coded for now
XORI_PATH = "/mnt/data/deps/xori/target/release/xori"
XORI_CONF_PATH = "/mnt/data/xori.json"


class Xori(PE):
    """
    """

    @staticmethod
    def process(sample_path):
        """ Extract PE feature data and xori ouotput
        :param sample_path:
        :return:
        """
        sample = Xori(sample_path)
        try:
            # calculate static pe features
            sample.extract()
            # get mnemonicss
            sample.disassemble()
        except Exception as e:
            log.exception("%s: %s", sample.local_path, e)
        else:
            return sample.sha256, sample.serialize()

    def disassemble(self):
        with self.tempfile() as tmp_path:
            try:
                output_base_name = os.path.basename(tmp_path)
                _ = subprocess.check_output(
                    [XORI_PATH, '--config', XORI_CONF_PATH, '-f', tmp_path,
                     '--output', '/tmp/'], stderr=subprocess.STDOUT, env={'XORI_PATH': XORI_PATH, 'XORI_CONF_PATH': XORI_CONF_PATH})
            except subprocess.CalledProcessError as e:
                log.error("%s: %s", self.sha256, e)
            else:
                mnemonics = {}
                with open("/tmp/%s_disasm.json" % output_base_name, 'r') as fin:
                    data = json.load(fin)
                    for e in data.values():
                        m = e['instr']['mnemonic']
                        if m in mnemonics:
                            mnemonics[m] += 1
                        else:
                            mnemonics[m] = 1
                self.add('mnemonics', mnemonics)