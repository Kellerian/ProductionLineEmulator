import sys
import socket
import argparse
from threading import Thread
from time import sleep
from collections import deque
from random import randint
from pathlib import Path
import logging
from typing import Optional


class PrinterEmul:
    def __init__(self, name, dm_list: deque, port: int):
        self.SIZE = 4096
        self.name = name
        self.FORMAT = "utf-8"
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setblocking(False)
        self.port = port
        self.server.bind(("", port))
        self.server.listen(5)
        self.dm_list = dm_list
        self.thread = Thread(target=self.run)

    def receive_all(self, s):
        data_all = b''
        while True:
            data = s.recv(self.SIZE)
            if len(data) > 0:
                data_all += data
                if len(data) < self.SIZE:
                    break
            else:
                break
        return data_all.decode(self.FORMAT)

    def start(self):
        self.thread.start()

    def run(self):
        connections = []
        i = 1
        while 1:
            try:
                connected_client, address = self.server.accept()
                connected_client.setblocking(True)
                connections.append(connected_client)
            except BlockingIOError:
                pass

            for client in connections:
                msg_received = ''
                try:
                    msg_received = self.receive_all(client)
                except ConnectionAbortedError:
                    pass
                    continue
                except Exception as e:
                    logging.error(f"[#{i}] <{self.name}> ERROR: {client} {e}")
                    connections.remove(client)
                if not msg_received:
                    continue
                msg_rows = msg_received.split("\n")
                r = 0
                for row in msg_rows:
                    dm_extracted = ''
                    if 'BARCODE=' in row:
                        row = row.replace('BARCODE=', '')
                        row = row.replace('~d034', '"')
                        dm_extracted = row.strip()
                    elif 'DMATRIX 10,10,400,400,c126,' in row:
                        row = row.replace('DMATRIX 10,10,400,400,c126,', '')
                        row = row.replace('~d034', '"')
                        dm_extracted = row[1:-1].strip()
                    elif 'XRB0,0,6,0,' in row:
                        row = msg_rows[r + 1]
                        dm_extracted = row.strip()
                    elif 'BR,24,24,2,5,250,0,1,' in row:
                        row = row.replace('BR,24,24,2,5,250,0,1,', '')
                        row = row.replace('~d034', '"')
                        dm_extracted = row.strip()
                    elif '^FH^FD_7e' in row:
                        row = row.replace('^FH^FD_7e', '')
                        row = row.replace('^FS', '')
                        dm_extracted = row.strip()
                    if dm_extracted.startswith("~1"):
                        dm_extracted = dm_extracted[2:]
                    r += 1
                    if dm_extracted:
                        logging.info(f"[#{i}] <{self.name}> "
                                     f"PRINTED: {dm_extracted}")
                        self.dm_list.append(dm_extracted)
                        i += 1
            sleep(0.01)


class FilePrinterEmul:
    def __init__(self, name, dm_list: deque, dm_file_path: Path):
        self.name = name
        self.dm_list = dm_list
        self.thread = Thread(target=self.run)
        self.dm_file_path = dm_file_path

    def start(self):
        self.thread.start()

    def run(self):
        with open(self.dm_file_path, 'r') as f:
            sleep(5)
            for line in f:
                self.dm_list.append(line.strip())
                sleep(0.02)


class TcpExchanger:
    def __init__(self, name: str, codes_to_send: deque,
                 transfer_buffer: list[deque] = None,
                 listen_port: int = 23,
                 timeout: float = 0.15, can_stop: bool = False,
                 gen_errors: bool = False, gen_duplicates: bool = False,
                 error_percent: int = 2,
                 stack=1, drop_dm_percent: int = 0):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setblocking(False)
        self.port = listen_port
        self.server.bind(("", listen_port))
        self.server.listen(5)
        self.connections = []
        self.can_stop = can_stop
        self.codes = codes_to_send
        self.transfer_buffer = transfer_buffer
        self.timeout = timeout
        self.name = name
        self.gen_errors = gen_errors
        self.error_percent = error_percent
        self.gen_duplicates = gen_duplicates
        self.thread = Thread(target=self.run)
        self.stack = stack
        self.drop_dm_percent = drop_dm_percent
        self.stack_pool = []

    def start(self):
        self.thread.start()

    def run(self):
        i = 0
        while 1:
            try:
                connected_client, address = self.server.accept()
                connected_client.setblocking(False)
                self.connections.append(connected_client)
            except BlockingIOError:
                pass
            if len(self.connections):
                sleep(self.timeout)
                if not self.codes:
                    continue
                if self.gen_errors and randint(0, 100) <= self.error_percent:
                    message = 'error'
                else:
                    message = self.codes.popleft()
                if (
                    self.gen_duplicates and
                    randint(0, 100) <= self.error_percent
                ):
                    message = "\n\r".join([message, message])
                self.stack_pool.append(message)
                if len(self.stack_pool) == self.stack:
                    message = "\n\r".join(self.stack_pool)
                    self.stack_pool.clear()
                else:
                    continue
                logging.info(
                    f"[{len(self.codes)}]<{self.name}> "
                    f"SENT: {message.strip()}"
                )

                if self.drop_dm_percent and randint(0, 100) <= self.drop_dm_percent:
                    logging.info(f"[{len(self.codes)}]<{self.name}> "
                                 f"DROPPED: {message.strip()}")
                else:
                    for client in self.connections:
                        try:
                            client.send(bytes(message + "\n\r", 'utf-8'))
                        except ConnectionAbortedError:
                            continue
                        except Exception as e:
                            logging.warning(f"[{len(self.codes)}]<{self.name}>"
                                            f" ERROR: {client} {e}")
                            self.connections.remove(client)

                if self.transfer_buffer is not None:
                    if 'error' not in message:
                        self.transfer_buffer[i].append(message)
                        i += 1
                        if i >= len(self.transfer_buffer):
                            i = 0
            sleep(0.01)


class SerialisationSetup:
    def __init__(self, printer_port: int, camera_port: int,
                 gen_errors: bool = False, error_percent: int = 2,
                 drop_dm_percent: int = 0):
        self.agr_buffer = list()
        self.dm_list = deque([])
        self.dm_printer = PrinterEmul('PRNSER', self.dm_list, printer_port)
        self.dm_camera = TcpExchanger(
            name="DMSER",
            codes_to_send=self.dm_list,
            transfer_buffer=self.agr_buffer,
            listen_port=camera_port,
            gen_errors=gen_errors,
            error_percent=error_percent,
            drop_dm_percent=drop_dm_percent
        )

    def run(self):
        logging.info(f"Started DM printer at port {self.dm_printer.port}...")
        self.dm_printer.start()
        logging.info(f"Started DM camera at port {self.dm_camera.port}...")
        self.dm_camera.start()


class SerialisationFromFileSetup:
    def __init__(self, camera_port: int, gen_errors: bool = False,
                 error_percent: int = 2, drop_dm_percent: int = 0):
        self.agr_buffer = list()
        self.dm_list = deque([])
        if getattr(sys, 'frozen', False):
            curPath = Path(sys.executable).parents[0]
        else:
            curPath = Path(__file__).parents[0]

        path_out = curPath / 'dm.csv'
        self.dm_file_path = path_out
        self.dm_printer = FilePrinterEmul('PRNSER', self.dm_list, self.dm_file_path)
        self.dm_camera = TcpExchanger(
            "DMSER", self.dm_list,
            transfer_buffer=self.agr_buffer,
            listen_port=camera_port,
            gen_errors=gen_errors,
            error_percent=error_percent,
            drop_dm_percent=drop_dm_percent
        )

    def run(self):
        logging.info(f"DM source file path {self.dm_file_path}...")
        self.dm_printer.start()
        logging.info(f"Started DM camera at port {self.dm_camera.port}...")
        self.dm_camera.start()


class AggregationVerificationSetup:
    def __init__(self, printer_port: int, camera_port: int):
        self.dm_list = deque([])
        self.dm_printer = PrinterEmul('PRNAGR', self.dm_list, printer_port)
        self.dm_camera = TcpExchanger(
            "VERIF", self.dm_list, can_stop=True,
            listen_port=camera_port, timeout=0.25,
            gen_errors=False, gen_duplicates=False
        )

    def run(self):
        logging.info("Started Aggregation printer at "
                     f"port {self.dm_printer.port}...")
        self.dm_printer.start()
        logging.info("Started Aggregation verification camera "
                     f"at port {self.dm_camera.port}...")
        self.dm_camera.start()


class AggregationSetup:
    def __init__(self, start_port: int,
                 agr_buffer: list[deque],
                 count: int = 1):
        self.agr_cam_list = dict[str, TcpExchanger]()
        self.start_port = start_port
        self.agr_buffer = agr_buffer
        self.count = count
        self.default_timeout = 0.25
    
    def gen_cameras(self):
        for i in range(self.count):
            self.agr_buffer.append(deque([]))
            cam_name = f"AGR_{i}"
            self.agr_cam_list[cam_name] = TcpExchanger(
                name=cam_name, codes_to_send=self.agr_buffer[i],
                listen_port=self.start_port + i, timeout=self.default_timeout
            )

    def run(self):
        self.gen_cameras()
        for camera, camera_obj in self.agr_cam_list.items():
            logging.info(f"Starting aggregation multicamera {camera} at "
                         f"port {camera_obj.port}...")
            camera_obj.start()


class PalletPrinter:
    def __init__(self, port: int):
        self.data = deque([])
        self.printer = PrinterEmul('PRNPAL', self.data, port)

    def run(self):
        logging.info(f"Starting palette printer at port {self.printer.port}..")
        self.printer.start()


class RefubrishingSetup:
    def __init__(self, camera_port: int, dm_file: Path):
        self.dm_file = dm_file
        self.dm_list = deque([])
        self.dm_camera = TcpExchanger(
            "DMREF", self.dm_list,
            listen_port=camera_port
        )

    def load_dm_from_file(self):
        with open(self.dm_file, 'r') as f:
            for line in f:
                self.dm_list.append(line.strip())
        return len(self.dm_list)

    def run(self):
        logging.info(f"Started DM camera at port {self.dm_camera.port}...")
        self.dm_camera.start()


def main_ser(args):
    agr_count = args.agr_count
    gen_err = args.gen_err
    perc_err = args.perc_err
    dm_file = args.dm_file
    drop_dm = args.drop_dm
    if dm_file:
        sr = SerialisationFromFileSetup(23, gen_err, perc_err, drop_dm)
    else:
        sr = SerialisationSetup(9101, 23, gen_err, perc_err, drop_dm)
    agr_setup = AggregationSetup(27, sr.agr_buffer, agr_count)
    agr_ver = AggregationVerificationSetup(9102, 32)
    p = PalletPrinter(9103)

    sr.run()
    agr_setup.run()
    agr_ver.run()
    p.run()


def main_refub(args):
    if getattr(sys, 'frozen', False):
        curPath = Path(sys.executable).parents[0]
    else:
        curPath = Path(__file__).parents[0]

    path_out = curPath / 'dm.csv'
    if not path_out.exists():
        logging.error(f'{path_out} does not exist')
        return
    
    rf = RefubrishingSetup(23, path_out)
    dms = rf.load_dm_from_file()
    logging.info(f'?????????????????? {dms} ???? ?????? ????????????????????.')
    rf.run()


if __name__ == '__main__':
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt='%(asctime)s [%(name)s][%(levelname)s]:'
                                  ' %(message)s', datefmt='%d.%m.%Y %H:%M:%S')
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    parser = argparse.ArgumentParser(
        prog='???????????????? ???????????????????????? ??????????',
        description='?????????????????? ???????????? ???????????????????????? ?????????? ???? '
        '???????????????????????? ?????????????????????????? ??????????????????',
        epilog='help - ???????????????????? ???? ??????????????????????????'
    )
    subparsers = parser.add_subparsers(help="?????????????????? ??????????????")
    parser_s = subparsers.add_parser('s', help='???????????? ?? ???????????? ????????????????????????')
    parser_s.add_argument(
        '-f', '--dm_file', choices=(0, 1), required=False, type=int,
        default=0, help='???????????????????? ?????????? ???? ?????????? dm.csv. ???????????????????????? ?????? ???????????????? ?????????? ?????? ???????????? ????.'
    )
    parser_s.add_argument(
        '-a', '--agr_count', choices=range(1, 10), required=False, type=int,
        default=3, help='???????????????????? ?????????? ?????????????????? ???? 1 ???? 9'
    )
    parser_s.add_argument(
        '-g', '--gen_err', choices=(0, 1), required=False, type=int,
        default=0, help='???????????????????????? ???????????? ????????????????????????: 0 - ??????, 1 - ????'
    )
    parser_s.add_argument(
        '-e', '--perc_err', choices=range(1, 100), required=False, type=int,
        default=2, help='???????????????? ?????????? ????????????????????????'
    )
    parser_s.add_argument(
        '-d', '--drop_dm', choices=range(0, 6), required=False, type=int,
        default=0, help='???????????????? ???????????????? ?????????? ???? ????????????????????????'
    )
    parser_s.set_defaults(func=main_ser)
    parser_r = subparsers.add_parser('r', help='???????????? ?? ???????????? ????????????????????')
    parser_r.set_defaults(func=main_refub)

    args = parser.parse_args()
    try:
        args.func(args)
    except AttributeError:
        parser.print_help()
        parser.exit()
