import sys
import select
import struct
import socket
import hashlib
from collections import deque

HEADER_FORMAT = 'I64s'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
FIXED_PKT_SIZE = 512
PORT = 5005


class FTPSender:

    def __init__(self, sock):
        self.sock = sock
        self.payload_size = FIXED_PKT_SIZE - HEADER_SIZE

    def receive_request(self):
        data, addr = self.sock.recvfrom(512)
        file, port = data.decode().split('!==!')
        port = int(port)
        print('[Sender]: Received request for: {}'.format(file))

        try:
            with open(file, 'rb') as f:
                self.file = f.read()
            self.total_pkts = len(self.file) // self.payload_size + 1

            # reply to request
            self.sock.sendto(str(self.total_pkts).encode(), (addr[0], port))
            print('[Sender]: Responded to request.')
            self.send(addr[0], port)
        except FileNotFoundError:
            self.sock.sendto(b'FNF', (addr[0], port))
            print('[Sender]: File not found, reply sent.')

    def _construct_packet(self, seq):
        eof = False
        if (seq + 1) * self.payload_size > len(self.file):
            stop = len(self.file) - 1
            eof = True
        else:
            stop = (seq + 1) * self.payload_size

        payload = self.file[seq * self.payload_size:stop]
        cksum = hashlib.sha256(payload).hexdigest().encode()
        pkt = struct.pack(HEADER_FORMAT, seq, cksum) + payload

        return pkt, eof

    def _send_one_packet(self, dest, port, seq):
        pkt, eof = self._construct_packet(seq)

        if len(pkt) == 68:
            return -1, True

        self.sock.sendto(pkt, (dest, port))
        print('[Sender]: Sent pkt #{}, size: {}.'.format(seq, len(pkt)))
        ready = select.select([self.sock], [], [], 5)
        if ready[0]:
            ack, _ = self.sock.recvfrom(10)
            ack = int(ack.decode().split('!==!')[1])
#            print('[Sender]: Received Ack #{}'.format(ack))

            return ack, eof
        else:
            print('[Sender]: Ack timed out, resending.') 
            return self._send_one_packet(dest, port, seq)

    def _handle_lost_packets(self, dest, port, lost):
        for p in lost:
            ack = p
            while ack == p:
                ack, _ = self._send_one_packet(dest, port, p)

    def send(self, dest, port):
        self.seq = 0
        dupAcks = [-1]
        eof = False
        cwnd = 1
        ssthrash = 100

        print('[Sender]: Sending file.')
        while True:
            cwnd = round(cwnd)
            print('[Sender]: Sending............................. CWND = {}'.format(cwnd))
            sent_this_rtt = []

            # sending one cwnd worth of packets each time
            for _ in range(round(cwnd)):
                if self.seq >= self.total_pkts:
                    self._handle_lost_packets(dest, port, set(dupAcks))
                    print('[Sender]: End of transmission.')
                    return

                ack, eof = self._send_one_packet(dest, port, self.seq)
                sent_this_rtt.append(self.seq)
                self.seq += 1
                if len(dupAcks) == 0 or ack == dupAcks[-1]:
                    if len(dupAcks) >= 3:

                        # resend lost packet, set ssthrash to cwnd/2 and slow start
                        ack, _ = self._send_one_packet(dest, port, dupAcks[-1])
                        ssthrash = int(cwnd / 2)
                        cwnd = 1
                        print('[Sender]: CWND reset to {}, ssthrash set to {}'.format(cwnd, ssthrash))
                        break
                    else:
                        dupAcks.append(ack)
                else:
                    dupAcks = []

                # growing CWND
                if cwnd < ssthrash:
                    cwnd += 1  # slow start
                else:
                    cwnd += 1 / cwnd  # collision avoidance

            if eof and len(dupAcks) == 1:
                break

        print('[Sender]: End of transmission.')


class FTPReceiver:

    def __init__(self, sock, addr, remote_port, self_port):
        self.sock = sock
        self.addr = addr
        self.remote_port = remote_port
        self.self_port = self_port

    def send_request(self, file):
        request = '{}!==!{}'.format(file, self.self_port)
        self.sock.sendto(request.encode(), (self.addr, self.remote_port))
        print('[Receiver]: Request sent.')
        pkts, _ = self.sock.recvfrom(10)
        if pkts == b'FNF':
            print('File not found. Please try again.')
        else:
            self.received = [b''] * int(pkts.decode())
            print('[Receiver]: Total # packets expected: {}'.format(pkts.decode()))
            self.receive(file)

    def receive(self, file):
        self.ack = 0
        need_resend = deque()
        newest_pkt = 0
        eof = False
        while True:
            pkt, _ = self.sock.recvfrom(FIXED_PKT_SIZE)
            header = pkt[:HEADER_SIZE]
            data = pkt[HEADER_SIZE:]
            seq, cksum = struct.unpack(HEADER_FORMAT, header)
            seq = int(seq)
            size = len(pkt)
            eof |= size < 512
            newest_pkt = max(newest_pkt, seq)

            if hashlib.sha256(data).hexdigest() == cksum.decode():

                # caching received data
                self.received[seq] = data

                if seq != self.ack:
                    print('[Receiver]: Expecting pkt #{}, received pkt #{}'.format(self.ack, seq))
                    for missed in range(self.ack, seq):
                        if missed not in need_resend and self.received[missed] == b'':
                            need_resend.append(missed)
                else:
                    print('[Receiver]: Received pkt #{}, checksum correct, size:{}.'.format(seq, size))
                    if len(need_resend) > 0 and seq == need_resend[0]:
                        need_resend.popleft()
                    if len(need_resend) == 0:
                        self.ack = newest_pkt + 1
                    else:
                        self.ack = need_resend[0]

            #            print('[Receiver]: Sending Ack #{}'.format(self.ack))
            self.sock.sendto('ACK!==!{}'.format(self.ack).encode(), (self.addr, self.remote_port))
            if eof and len(need_resend) == 0:
                break

        with open('./out/' + file, 'wb') as f:
            f.write(b''.join(self.received))


if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server = len(sys.argv) > 1 and sys.argv[1] == 'server'

    if server:
        sock.bind(('0.0.0.0', PORT))
        sender = FTPSender(sock)
        receiver = FTPReceiver(sock, '', PORT + 1, PORT)
        data, addr = sock.recvfrom(10)
        receiver.addr = addr[0]
        print('[Server]: Connection established with {}'.format(addr[0]))
        while True:
            data, _ = sock.recvfrom(512)
            mode, file = data.decode().split('!==!')
            if mode == 'get':
                sender.receive_request()
            elif mode == 'write':
                receiver.send_request(file)

    else:
        sock.bind(('0.0.0.0', PORT + 1))
        sender = FTPSender(sock)
        receiver = FTPReceiver(sock, '', PORT, PORT + 1)
        connected = False
        while True:
            cmd = input('myftp> ')
            if cmd.startswith('?'):
                print('Commands may be abbreviated.')
                print('connect        connect to remote myftp')
                print('put            send file')
                print('get            receive file')
                print('quit           exit tftp')
                print('?              print this help information')

            elif cmd.startswith('c'):
                server_addr = cmd.split()[1]
                receiver.addr = server_addr
                sock.sendto(b'', (server_addr, PORT))
                print('[Client] Connection established with {}.'.format(
                    server_addr))
                connected = True

            elif cmd.startswith('g'):
                sock.sendto(b'get!==!', (receiver.addr, PORT))
                receiver.send_request(cmd.split()[1])

            elif cmd.startswith('p'):
                sock.sendto('write!==!{}'.format(cmd.split()[1]).encode(),
                            (receiver.addr, PORT))
                sender.receive_request()

            elif cmd.startswith('q'):
                sys.exit(1)

            elif not connected and not cmd.startswith('c'):
                print('You are not connected to any server yet.')
