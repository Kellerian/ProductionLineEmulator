import sys
import socket
import argparse
from enum import Enum
from threading import Thread
from time import sleep
from collections import deque
from random import randint, choice
from pathlib import Path
import logging


PAUSE = 0.001


class CodeQuality(Enum):
    A = 'A'
    B = 'B'
    C = 'C'
    D = 'D'
    E = 'E'
    F = 'F'


GOOD_CODES = (CodeQuality.A.value, CodeQuality.B.value)


BAD_CODES = (
    CodeQuality.C.value,
    CodeQuality.D.value,
    CodeQuality.E.value,
    CodeQuality.F.value
)


class PrinterEmul:
    def __init__(
        self, name, dm_list: deque, port: int
    ):
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
        print_buffers = {}
        connections = []
        i = 1
        while 1:
            try:
                connected_client, address = self.server.accept()
                connected_client.setblocking(True)
                connections.append(connected_client)
                print_buffers[connected_client] = deque([])
            except BlockingIOError:
                pass

            for client in connections:
                msg_received = ''
                try:
                    msg_received = self.receive_all(client).strip()
                except ConnectionAbortedError:
                    continue
                except Exception as e:
                    logging.error(f"[#{i}] <{self.name}> ERROR: {client} {e}")
                    connections.remove(client)
                if not msg_received:
                    continue
                if msg_received == f"{chr(27)}!?":
                    client.send('\x00'.encode())
                    continue
                elif msg_received == "~S,CHECK":
                    client.send('00'.encode())
                    continue
                elif msg_received == "OUT @LABEL":
                    client.send(f"{i}".encode())
                    continue
                elif msg_received == "~S,LABEL":
                    client.send(f"{len(print_buffers[client])}".encode())
                    continue
                else:
                    msg_rows = msg_received.split("\n")
                    r = 0
                    dm_extracted = ''
                    for row in msg_rows:
                        if 'BARCODE=' in row:
                            row = row.replace('BARCODE=', '')
                            row = row.replace('~d034', '"')
                            dm_extracted = row.strip()
                        elif 'DMATRIX' in row or "BARCODE " in row:
                            params = row.split(",")
                            row = params[-1]
                            row = row.replace('~d034', '"')
                            dm_extracted = row[1:-1].strip()
                        elif 'XRB0,0,' in row:
                            row = msg_rows[r + 1]
                            dm_extracted = row.strip()
                        elif 'BR,24,24' in row:
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
                            i += 1
                            if '05060367340398' in dm_extracted:
                                volume = randint(100, 1000)
                                dm_extracted = (
                                    f"{dm_extracted}{chr(29)}3353{volume:06}"
                                )
                            elif '07808631857726' in dm_extracted:
                                weight = randint(100, 1000)
                                dm_extracted = (
                                    f"{dm_extracted}{chr(29)}3103{weight:06}"
                                )
                            logging.info(f"[#{i}] <{self.name}> "
                                         f"PRINTED: {dm_extracted}")
                            print_buffers[client].append(dm_extracted)
                            break
                    else:
                        if not dm_extracted:
                            logging.debug(f"DATA INPUT: {msg_received}")
                try:
                    self.dm_list.append(print_buffers[client].popleft())
                except IndexError:
                    pass
            sleep(PAUSE)


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
    def __init__(
        self, name: str,
        codes_to_send: deque,
        transfer_buffer: list[deque],
        listen_port: int = 23,
        timeout: float = 0.05,
        can_stop: bool = False,
        gen_errors: bool = False,
        error_percent: int = 2,
        stack=1,
        drop_dm_percent: int = 0,
        add_code_quality: bool = False,
        bad_codes_percent: int = 0
    ):
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
        self.add_code_quality = add_code_quality
        self.bad_codes_percent = bad_codes_percent
        self.error_percent = error_percent
        self.thread = Thread(target=self.run)
        self.stack = stack
        self.drop_dm_percent = drop_dm_percent
        self.delay = False
        self.stack_pool = []

    def start(self, delay=False):
        self.delay = delay
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
            if self.connections:
                sleep(self.timeout)
                if not self.codes:
                    continue
                orig_code = None
                if self.gen_errors and randint(0, 100) <= self.error_percent:
                    if randint(0, 1):
                        message = 'error'
                    else:
                        message = self.codes.popleft()
                        orig_code = message
                        message = "\n\r".join([message, message])
                else:
                    message = self.codes.popleft()
                    orig_code = message
                    if self.add_code_quality:
                        if randint(0, 100) <= self.bad_codes_percent:
                            message += f'@{choice(BAD_CODES)}'
                        else:
                            message += f'@{choice(GOOD_CODES)}'
                self.stack_pool.append(message)
                if len(self.stack_pool) == self.stack:
                    message = "\n\r".join(self.stack_pool)
                    self.stack_pool.clear()
                else:
                    continue
                if (
                    self.drop_dm_percent and randint(0, 100) <=
                    self.drop_dm_percent
                ):
                    logging.info(
                        f"[{self.name}]<{len(self.codes)}> "
                        f"DROPPED: {message.strip()}"
                    )
                else:
                    for client in self.connections:
                        try:
                            client.send(bytes(message + "\n\r", 'utf-8'))
                            logging.info(
                                f"[{self.name}]<{len(self.codes)}> "
                                f"SENT: {message.strip()}"
                            )
                        except ConnectionAbortedError:
                            continue
                        except Exception as e:
                            logging.warning(
                                f"[{self.name}]<{len(self.codes)}>"
                                f" ERROR: {client} {e}"
                            )
                            self.connections.remove(client)

                if self.transfer_buffer:
                    if orig_code is not None:
                        if self.add_code_quality:
                            orig_code = orig_code[:-2]
                        self.transfer_buffer[i].append(orig_code)
                        i += 1
                        if i >= len(self.transfer_buffer):
                            i = 0
            sleep(PAUSE)


class SerialisationSetup:
    def __init__(self, printer_port: int, camera_port: int,
                 gen_errors: bool = False, error_percent: int = 2,
                 drop_dm_percent: int = 0,
                 read_interval: float = 0.15,
                 add_code_quality: bool = False,
                 bad_codes_percent: int = 0):
        self.agr_buffer = list()
        self.dm_list = deque([])
        self.dm_printer = PrinterEmul(
            'PRNSER', self.dm_list, printer_port
        )
        self.dm_camera = TcpExchanger(
            name="DMSER",
            timeout=read_interval,
            codes_to_send=self.dm_list,
            transfer_buffer=self.agr_buffer,
            listen_port=camera_port,
            gen_errors=gen_errors,
            error_percent=error_percent,
            drop_dm_percent=drop_dm_percent,
            add_code_quality=add_code_quality,
            bad_codes_percent=bad_codes_percent
        )

    def run(self):
        logging.info(f"Started DM printer at port {self.dm_printer.port}...")
        self.dm_printer.start()
        logging.info(f"Started DM camera at port {self.dm_camera.port}...")
        self.dm_camera.start(delay=True)


class SerialisationFromFileSetup:
    def __init__(self, camera_port: int, gen_errors: bool = False,
                 error_percent: int = 2, drop_dm_percent: int = 0,
                 read_interval: float = 0.15,
                 add_code_quality: bool = False,
                 bad_codes_percent: int = 0):
        self.agr_buffer = list()
        self.dm_list = deque([])
        if getattr(sys, 'frozen', False):
            cur_path = Path(sys.executable).parents[0]
        else:
            cur_path = Path(__file__).parents[0]

        path_out = cur_path / 'dm.csv'
        self.dm_file_path = path_out
        self.dm_printer = FilePrinterEmul(
            'PRNSER', self.dm_list, self.dm_file_path
        )
        self.dm_camera = TcpExchanger(
            name="DMSER",
            codes_to_send=self.dm_list,
            transfer_buffer=self.agr_buffer,
            timeout=read_interval,
            listen_port=camera_port,
            gen_errors=gen_errors,
            error_percent=error_percent,
            drop_dm_percent=drop_dm_percent,
            add_code_quality=add_code_quality,
            bad_codes_percent=bad_codes_percent
        )

    def run(self):
        logging.info(f"DM source file path {self.dm_file_path}...")
        self.dm_printer.start()
        logging.info(f"Started DM camera at port {self.dm_camera.port}...")
        self.dm_camera.start()


class AggregationVerificationSetup:
    def __init__(
        self,
        printer_port: int,
        camera_port: int,
        read_interval: float = 0.05
    ):
        self.dm_list = deque([])
        self.dm_printer = PrinterEmul('PRNAGR', self.dm_list, printer_port)
        self.dm_camera = TcpExchanger(
            name="VERIF",
            codes_to_send=self.dm_list,
            can_stop=True,
            listen_port=camera_port,
            timeout=read_interval,
            gen_errors=False,
            transfer_buffer=[]
        )

    def run(self):
        logging.info("Started Aggregation printer at "
                     f"port {self.dm_printer.port}...")
        self.dm_printer.start()
        logging.info("Started Aggregation verification camera "
                     f"at port {self.dm_camera.port}...")
        self.dm_camera.start()


class AggregationSetup:
    def __init__(self, start_port: int, agr_buffer: list[deque],
                 count: int = 1, read_interval: float = 0.05):
        self.agr_cam_list = dict[str, TcpExchanger]()
        self.start_port = start_port
        self.agr_buffer = agr_buffer
        self.count = count
        self.default_timeout = read_interval
    
    def gen_cameras(self):
        for i in range(self.count):
            self.agr_buffer.append(deque([]))
            cam_name = f"AGR_{i}"
            self.agr_cam_list[cam_name] = TcpExchanger(
                name=cam_name, codes_to_send=self.agr_buffer[i],
                listen_port=self.start_port + i, timeout=self.default_timeout,
                transfer_buffer=[]
            )

    def run(self):
        self.gen_cameras()
        for camera, camera_obj in self.agr_cam_list.items():
            logging.info(f"Starting aggregation multicamera {camera} at "
                         f"port {camera_obj.port}...")
            camera_obj.start(delay=True)


class PalletPrinter:
    def __init__(self, port: int, name: str):
        self.data = deque([])
        self.printer = PrinterEmul(name, self.data, port)
        self.name = name

    def run(self):
        logging.info(f"Starting {self.name} printer at port"
                     f" {self.printer.port}..")
        self.printer.start()


class RefubrishingSetup:
    def __init__(self, camera_port: int, dm_file: Path):
        self.dm_file = dm_file
        self.dm_list = deque([])
        self.dm_camera = TcpExchanger(
            name="DMREF",
            codes_to_send=self.dm_list,
            listen_port=camera_port,
            transfer_buffer=[]
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
    gen_err = bool(args.gen_err)
    perc_err = args.perc_err
    dm_file = args.dm_file
    drop_dm = args.drop_dm
    read_interval = args.read_interval
    add_code_quality = bool(args.add_code_quality)
    bad_code_quality_percent = args.bad_code_quality_percent

    if dm_file:
        sr = SerialisationFromFileSetup(
            23,
            gen_errors=gen_err,
            error_percent=perc_err,
            drop_dm_percent=drop_dm,
            read_interval=read_interval,
            add_code_quality=add_code_quality,
            bad_codes_percent=bad_code_quality_percent
        )
    else:
        sr = SerialisationSetup(
            9101, 23,
            gen_errors=gen_err,
            error_percent=perc_err,
            drop_dm_percent=drop_dm,
            read_interval=read_interval,
            add_code_quality=add_code_quality,
            bad_codes_percent=bad_code_quality_percent,

        )
    sr.run()

    if agr_count:
        agr_setup = AggregationSetup(
            27, sr.agr_buffer, agr_count, read_interval=read_interval
        )
        agr_setup.run()
        agr_ver = AggregationVerificationSetup(
            9102, 32, read_interval=read_interval
        )
        agr_ver.run()
    else:
        PalletPrinter(9102, "LEVEL_0").run()
    PalletPrinter(9103, "LEVEL_1").run()
    PalletPrinter(9104, "LEVEL_2").run()
    PalletPrinter(9105, "LEVEL_3").run()


def main_refub(_):
    if getattr(sys, 'frozen', False):
        cur_path = Path(sys.executable).parents[0]
    else:
        cur_path = Path(__file__).parents[0]

    path_out = cur_path / 'dm.csv'
    if not path_out.exists():
        logging.error(f'{path_out} does not exist')
        return
    
    rf = RefubrishingSetup(23, path_out)
    dms = rf.load_dm_from_file()
    logging.info(f'Загружено {dms} км для отбраковки.')
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
        prog='Эмулятор промышленной линии',
        description='Эмулирует работу промышленной линии на '
        'производстве маркированной продукции',
        epilog='help - информация по использованию'
    )
    subparsers = parser.add_subparsers(help="Параметры запуска")
    # Парсер настроек режима сериализации
    parser_s = subparsers.add_parser('s', help='Запуск в режиме сериализации')
    parser_s.add_argument(
        '-f', '--dm_file', choices=(0, 1), required=False, type=int,
        default=0,
        help='Передавать марки из файла dm.csv. '
             'Используется для эмуляции линии без печати КМ.'
    )
    parser_s.add_argument(
        '-a', '--agr_count', choices=range(0, 10), required=False, type=int,
        default=3, help='Количество камер агрегации от 0 до 9'
    )
    parser_s.add_argument(
        '-g', '--gen_err', choices=(0, 1), required=False, type=int,
        default=0, help='Генерировать ошибки сериализации: 0 - нет, 1 - да'
    )
    parser_s.add_argument(
        '-e', '--perc_err', choices=range(1, 100), required=False, type=int,
        default=2, help='Процентр брака сериализации'
    )
    parser_s.add_argument(
        '-d', '--drop_dm', choices=range(0, 6), required=False, type=int,
        default=0, help='Процентр пропуска кодов на сериализации'
    )
    parser_s.add_argument(
        '-r', '--read_interval', required=False, type=float,
        default=0.15,
        help='Интервал передачи кодов маркировки (сек), например 0.15 = 150мс'
    )
    parser_s.add_argument(
        '-q', '--add_code_quality', choices=(0, 1), required=False, type=int,
        default=0,
        help='Добавлять флаг качества кода в конец КМ: 0 - нет, 1 - да'
    )
    parser_s.add_argument(
        '-qe', '--bad_code_quality_percent',
        choices=range(1, 100), required=False, type=float,
        default=0.15, help='Процент кодов плохого качества (ниже B)'
    )
    parser_s.set_defaults(func=main_ser)

    # Парсер для активации режима отбраковки
    parser_r = subparsers.add_parser('r', help='Запуск в режиме отбраковки')
    parser_r.set_defaults(func=main_refub)

    cmd_arguments = parser.parse_args()
    try:
        cmd_arguments.func(cmd_arguments)
    except AttributeError:
        parser.print_help()
        parser.exit()
