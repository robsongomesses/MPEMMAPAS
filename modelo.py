import argparse
import gzip
import os
import re
import requests

from datetime import datetime
from hashlib import md5

from io import BytesIO


class File:

    url = 'https://datalakecadg.mprj.mp.br/api/upload/'

    def __init__(self, file_path, filename=None):
        self.file_path = file_path
        self.file_obj = self._load(file_path)
        self.filename = filename

    def _load(self, file_path):
        with open(file_path, 'rb') as fobj:
            buf = BytesIO()
            buf.write(fobj.read())
            buf.seek(0)
            return buf

    def calc_md5(self, compressed_file):
        md5_obj = md5(compressed_file)
        return md5_obj.hexdigest()

    def compress(self):
        return gzip.compress(
            self.file_obj.read()
        )

    @property
    def _prep_name(self):
        base_name = os.path.basename(self.file_path)
        prep_name = re.sub(
            '(.csv|[-:]|\s+)',
            '',
            base_name + '_' + str(datetime.now())[:19]
        )
        return prep_name + '.csv.gz'

    def _process_response(self, resp):
        if resp.status_code == 200:
            print('File uploaded successfully!')
        else:
            msg = re.sub(
                r'<.*>(.*)</?.*>',
                '\g<1>',
                resp.content.decode()
            )
            print('Status:', resp.status_code, msg)

    def post(self, username, method, secret):
        compressed = self.compress()
        md5digest = self.calc_md5(compressed)

        payload = {
            'filename': self.filename or self._prep_name,
            'nome': username,
            'md5': md5digest,
            'method': method,
            'SECRET': secret
        }
        self.resp = requests.post(
            self.url,
            files={'file': (self._prep_name, compressed)},
            data=payload
        )
        self._process_response(self.resp)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'filepath',
        help='Caminho completo até o arquivo que se deseja enviar'
    )
    parser.add_argument(
        'methodname',
        help='Nome do método do respectivo arquivo'
    )
    parser.add_argument(
        'username',
        help='Nome do usuário cadastrado no sistema'
    )
    parser.add_argument(
        'secretkey',
        help='Chave secreta que autentica o usuário'
    )
    parser.add_argument(
        '--filename',
        help='Nome do arquivo que será salvo no data lake.'
    )
    args = parser.parse_args()

    filepath = args.filepath
    filename = args.filename
    methodname = args.methodname
    username = args.username
    secretkey = args.secretkey

    file_obj = File(filepath, filename)

    # Send data
    file_obj.post(username, methodname, secretkey)
