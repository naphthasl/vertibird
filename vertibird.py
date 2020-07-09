#!/usr/bin/env python
"""
Vertibird is a dead-simple virtualization library based around direct access to
QEMU. Because I couldn't be bothered to figure out how the hell the crappy
libvirt C bindings work. I also couldn't be bothered to write a million lines
of XML. Screw that.

Currently only tested on x86_64, and probably only works on x86_64. Usage is
also only possible on Linux, this library is NOT cross-platform. It would've
taken an infinitely longer amount of time to write this for Windows, since it
makes use of features such as named pipes.

PyPi: https://pypi.org/project/vertibird/

GitHub & Further Information: https://github.com/naphthasl/vertibird

License: MIT (see LICENSE for details)
"""

import shelve, threading, time, uuid, socket, subprocess, os, telnetlib, signal
import sys, shlex, random, string, psutil, zlib, builtins, select, tempfile, io
import ipaddress, zmq, collections, traceback, multiprocessing

from contextlib import closing

from vncdotool import api as vncapi
from PIL import Image, ImageDraw
from filelock import Timeout, FileLock, SoftFileLock
from dateutil.tz import tzlocal
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, PickleType, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import ObjectDereferencedError

__author__ = 'Naphtha Nepanthez'
__version__ = '0.0.17'
__license__ = 'MIT' # SEE LICENSE FILE
__all__ = [
    'Vertibird',
    'SessionManager',
    'session_generator',
    'QEMUDevices',
    'Exceptions'
]

# TODO: Allow CD-ROMs and floppy disks to be removed and inserted during
# operation

# TODO: Perhaps create some sort of MKV/MP4/whatever stream out of the VM's
# video and audio output?
# [POTENTIALLY OUT OF SCOPE]

# TODO: Allow for multiple network devices, some of which may not use
# usermode networking

# TODO: Find some way to support "picky" operating systems like Mac OS
# [POTENTIALLY OUT OF SCOPE]

# TODO: Support more than 2 attached IDE devices with multiple IDE buses.

# Connection details (note: QEMU_VNC_ADDS is related to a quirk with QEMU)
GLOBAL_LOOPBACK = '127.0.0.1'
QEMU_VNC_ADDS = 5900

# Connection timeouts
TELNET_TIMEOUT_SECS = 1
VNC_TIMEOUT_SECS = TELNET_TIMEOUT_SECS

# Realtime state check and non-realtime state check poll times
STATE_CHECK_CLK_SECS = 0.075
STATE_CHECK_NRLT_CLK_SECS = 0.5
DB_CINTERVAL = 0.2
VNC_FRAMERATE = 60
CPU_USAGE_INTERVAL = 1

# Audio options
AUDIO_CLEAR_INTERVAL = 1
AUDIO_MAX_SIZE = 17640
AUDIO_BLOCK_SIZE = 4096

# Logging options
MAX_LOG_SIZE = 8192

# Function defaults
DEFAULT_DSIZE = 8589934592
VNC_IMAGE_MODE = 'RGB'
DISK_FORMAT = 'qcow2'

# Customization
VNC_NO_SIGNAL_MESSAGE = 'Unable to retrieve frames from VNC server right now.'

# Debugging
DEBUG = False
EXPERIMENTAL_SHARED_INSTANCES = False

# Module requirements
BLANK_WAV_HEADER =\
    b'RIFF\x00\x00\x00\x00WAVEfmt \x10\x00\x00\x00\x01'\
    b'\x00\x02\x00D\xac\x00\x00\x10\xb1\x02\x00\x04\x00'\
    b'\x10\x00data\x00\x00\x00\x00'
GLOBAL_LOCK = multiprocessing.Lock()

class QEMUDevices(object):
    vga_arg = {
        # Deprecated
        'none': 'No Graphics',
        'std': 'Standard VGA',
        'cirrus': 'Cirrus VGA',
        'vmware': 'VMWare SVGA',
        'qxl': 'QXL VGA',
        'virtio': 'VirtIO VGA'
    }
    
    vga = {
        'bochs-display': 'Bochs Linear Framebuffer',
        'virtio-vga': 'VirtIO VGA',
        'virtio-vga,virgl=on': 'VirtIO VGA + VirGL',
        'qxl-vga': 'QXL VGA',
        'cirrus-vga': 'Cirrus CLGD 54xx VGA',
        'isa-cirrus-vga': 'Cirrus CLGD 54xx VGA (ISA)',
        'VGA': 'Standard VGA',
        'isa-vga': 'Standard VGA (ISA)',
        'ramfb': 'Guest Memory Framebuffer',
        'vmware-svga': 'VMWare SVGA-II (vmwgfx?)'
    }
    
    machine = {
        'pc': 'Standard PC (i440FX + PIIX, 1996)',
        'q35': 'Standard PC (Q35 + ICH9, 2009)',
        'isapc': 'ISA-Only PC'
    }
    
    sound = {
        'ac97': 'Intel 82801AA AC97 Audio',
        'hda': 'Intel HD Audio',
        'sb16': 'Creative Sound Blaster 16',
        'gus': 'Gravis Ultrasound GF1',
        'adlib': 'Yamaha YM3812 (OPL2)'
    }
    
    inputs = {
        'ps2': 'PS/2 Compatible Keyboard and Mouse',
        'usb-mouse': 'Standard USB Keyboard and Mouse',
        'usb-tablet': 'Standard USB Keyboard and Tablet'
    }
    
    network = {
        'e1000': 'Intel 82540EM Gigabit Ethernet',
        'e1000-82544gc': 'Intel 82544GC Gigabit Ethernet',
        'e1000-82545em': 'Intel 82545EM Gigabit Ethernet',
        'e1000e': 'Intel 82574L GbE Controller',
        'i82550': 'Intel i82550 Ethernet',
        'i82551': 'Intel i82551 Ethernet',
        'i82557a': 'Intel i82557A Ethernet',
        'i82557b': 'Intel i82557B Ethernet',
        'i82557c': 'Intel i82557C Ethernet',
        'i82558a': 'Intel i82558A Ethernet',
        'i82558b': 'Intel i82558B Ethernet',
        'i82559a': 'Intel i82559A Ethernet',
        'i82559b': 'Intel i82559B Ethernet',
        'i82559c': 'Intel i82559C Ethernet',
        'i82559er': 'Intel i82559ER Ethernet',
        'i82562': 'Intel i82562 Ethernet',
        'i82801': 'Intel i82801 Ethernet',
        'ne2k_isa': 'NE2000 ISA',
        'ne2k_pci': 'NE2000 PCI',
        'pcnet': 'PCNet-FAST III',
        'rtl8139': 'Realtek Semiconductor Corp RTL8139 Ethernet',
        'tulip': 'Generic Tulip PCI NIC',
        'virtio-net-pci': 'VirtIO Ethernet PCI',
        'virtio-net-pci-non-transitional': 'VirtIO Non-Transitional NIC PCI',
        'virtio-net-pci-transitional': 'VirtIO Transitional NIC PCI',
        'vmxnet3': 'VMWare Paravirtualized Ethernet v3'
    }
    
    scsi = {
        'lsi53c895a': 'LSILogic SCSI to PCI LSI53C895A',
        'am53c974': 'AMD Am53c974 PCscsi-PCI SCSI adapter',
        'dc390': 'Tekram DC-390 SCSI adapter',
        'lsi53c810': 'NCR/Symbios/LSI Logic 53C810 PCI SCSI host adapter',
        'mptsas1068': 'LSI SAS 1068',
        'megasas': 'LSI MegaRAID SAS 1078',
        'megasas-gen2': 'LSI MegaRAID SAS 2108',
        'virtio-scsi-pci': 'VirtIO SCSI PCI',
        'virtio-scsi-pci-non-transitional': 'VirtIO SCSI Non-Transitional',
        'virtio-scsi-pci-transitional': 'VirtIO SCSI Transitional'
    }
    
    rtc = {
        'utc': 'UTC Host Clock',
        'localtime': 'Localtime Host Clock ({0})'.format(
            datetime.now(tzlocal()).tzname()
        )
    }
    
    cpu = {
        'base': 'Base Processor (No features enabled)',
        'host': 'KVM Host Processor (All supported host features)',
        'max': 'KVM Max Processor (All supported accelerator features)',
        'qemu64': 'x86_64 QEMU Virtual CPU version 2.5+',
        'qemu32': 'x86 QEMU Virtual CPU version 2.5+',
        'phenom': 'AMD Phenom(tm) 9550 Quad-Core Processor',
        'pentium3': 'Intel Pentium 3',
        'pentium2': 'Intel Pentium 2',
        'pentium': 'Intel Pentium',
        'n270': 'Intel(R) Atom(TM) CPU N270 @ 1.60GHz',
        'kvm64': 'Common KVM Processor',
        'kvm32': 'Common 32-bit KVM Processor',
        'coreduo': 'Genuine Intel(R) CPU T2600 @ 2.16GHz',
        'core2duo': 'Intel(R) Core(TM)2 Duo CPU T7700 @ 2.40GHz',
        'athlon': 'AMD Athlon V1/QEMU Virtual CPU 2.5+',
        'Westmere-IBRS': 'Westmere E56xx/L56xx/X56xx (IBRS update)',
        'Westmere': 'Westmere E56xx/L56xx/X56xx (Nehalem-C)',
        'Snowridge': 'Intel Atom Processor (SnowRidge)',
        'SandyBridge': 'Intel Xeon E312xx (Sandy Bridge)',
        'SandyBridge-IBRS': 'Intel Xeon E312xx (Sandy Bridge, IBRS update)',
        'Penryn': 'Intel Core 2 Duo P9xxx (Penryn Class Core 2)',
        'Opteron_G5': 'AMD Opteron 63xx class CPU',
        'Opteron_G4': 'AMD Opteron 62xx class CPU',
        'Opteron_G3': 'AMD Opteron 23xx class CPU',
        'Opteron_G2': 'AMD Opteron 22xx class CPU',
        'Opteron_G1': 'AMD Opteron 240 class CPU',
        'Nehalem': 'Intel Core i7 9xx (Nehalem Class Core i7)',
        'Nehalem-IBRS': 'Intel Core i7 9xx (Nehalem Core i7, IBRS update)',
        'KnightsMill': 'Intel Xeon Phi Processor (Knights Mill)',
        'IvyBridge': 'Intel Xeon E3-12xx v2 (Ivy Bridge)',
        'IvyBridge-IBRS': 'Intel Xeon E3-12xx v2 (Ivy Bridge, IBRS)',
        'Haswell': 'Intel Core Processor (Haswell)',
        'Haswell-IBRS': 'Intel Core Processor (Haswell, IBRS)',
        'Haswell-noTSX': 'Intel Core Processor (Haswell, no TSX)',
        'Haswell-noTSX-IBRS': 'Intel Core Processor (Haswell, no TSX, IBRS)',
        'EPYC': 'AMD EPYC Processor',
        'EPYC-IBPB': 'AMD EPYC Processor (with IBPB)',
        'Dhyana': 'Hygon Dhyana Processor',
        'Denverton': 'Intel Atom Processor (Denverton)',
        'Conroe': 'Intel Celeron_4x0 (Conroe/Merom Class Core 2)',
        'Cascadelake-Server': 'Intel Xeon Processor (Cascadelake,+TSX)',
        'Cascadelake-Server-noTSX': 'Intel Xeon Processor (Cascadelake,-TSX)',
        'Broadwell': 'Intel Core Processor (Broadwell)',
        'Broadwell-noTSX': 'Intel Core Processor (Broadwell,-TSX)',
        'Broadwell-IBRS': 'Intel Core Processor (Broadwell,+IBRS)',
        'Broadwell-noTSX-IBRS': 'Intel Core Processor (Broadwell,-TSX,+IBRS)',
        '486': 'Intel 486 Processor'
    }
    
    storage = {
        'ahci': 'Standard AHCI Device/Serial ATA Device',
        'ide': 'Standard Machine-aliased IDE Device',
        'scsi': 'SCSI/SAS-Attached Device',
        'virtio': 'VirtIO Block Device'
    }

class Exceptions(object):
    class IncompatibleOperatingSystem(Exception):
        pass
        
    class InvalidDiskFormat(Exception):
        pass
        
    class DriveAlreadyExists(Exception):
        pass
        
    class InvalidStateChange(Exception):
        pass
        
    class LaunchDependencyMissing(Exception):
        pass
    
    class InvalidDriveType(Exception):
        pass
        
    class InvalidGenericDeviceType(Exception):
        pass
        
    class LimitReached(Exception):
        pass
        
    class VMLaunchException(Exception):
        pass
        
    class InvalidArgument(Exception):
        pass
        
    class SubprocessLaunchException(Exception):
        pass

class Vertibird(object):
    """
    WARNING: Be careful with what kinds of input you feed Vertibird. Everything
    will probably end up being fed to the commandline in some way or another
    due to the nature of QEMU. If you want my advice, you should ensure that
    device names, file paths, etc are kept short, restricted to alphanumeric
    characters only, and absolutely no characters such as the comma, space,
    semicolon, etc.
    
    Just ALWAYS remember that anything you input will be converted into
    commandline arguments for QEMU.
    """
    
    Base = declarative_base()

    def __init__(
                self,
                qemu: str = 'qemu-kvm',
                persistence: str = (
                    'sqlite:///vertibird.db' +
                    '?check_same_thread=False'
                )
            ):
        """
        Create a Vertibird hypervisor interface.
        
        If using SQLite...
         - Ensure that check_same_thread is set to False, otherwise SQLite will
        throw exceptions whenever a connection is garbage collected even
        though each database connection is stored thread-locally anyway. There
        is no risk to enabling this option.
        
        Parameters
        ----------
        qemu :
            The QEMU executable to use, defaults to qemu-kvm
        persistence :
            The shelf/database to store information about created VMs in,
            defaults to sqlite:///vertibird.db
        """
        
        # Not sure if this is a good idea
        if not ('linux' in sys.platform.lower()):
            raise Exceptions.IncompatibleOperatingSystem(
                'Only Linux is supported.'
            )
        
        self._local = threading.local()
        
        self.qemu = qemu
        self.db_info = persistence
        self.vm_instances = {}
    
    def __new_db(self):
        engine = create_engine(
            self.db_info,
            isolation_level='READ UNCOMMITTED'
        )
        self.Base.metadata.create_all(engine)
        self._local.db = (sessionmaker(
            bind = engine
        ))()
    
    def db(self):
        if not hasattr(self._local, 'db'):
            self.__new_db()
        
        if ((not self._local.db.is_active)
            or self._local.db.connection().closed
            or self._local.db.deleted):
                
            self._local.db.close()
            self.__new_db()
            
        return self._local.db
    
    def create(self):
        """
        Creates a virtual machine with a random UUID and returns the live
        access object.
        """
        x = self.VertiVM(id = str(uuid.uuid4()), ports = self._new_ports())
        self.db().add(x)
        self.db().commit()
        
        return self.__wrap_live(x)
        
    def get(self, vmuuid = str):
        """
        Retrieves a Vertibird.VertiVMLive object via the given UUID.
        """
        return self.__wrap_live(self.db().query(self.VertiVM).filter(
            self.VertiVM.id == vmuuid
        ).one())
    
    def remove(self, vmuuid = str):
        """
        Removes a virtual machine of given UUID.
        """
        self.get(vmuuid).remove()
        
    def create_drive(self, img: str, size: int = DEFAULT_DSIZE):
        """
        Creates a virtual machine drive at the specified path and of the
        specified size. Size is in bytes and defaults to DEFAULT_DSIZE.
        
        Virtual machine drives are created as sparse files, so ensure that your
        filesystem supports them - it's an exceedingly useful feature, and
        contributes to the performance of Vertibird significantly!
        """
        if not os.path.isfile(img):
            if DISK_FORMAT == 'raw':
                f = open(img, 'wb')
                f.truncate(size)
                f.close()
            elif DISK_FORMAT == 'qcow2':
                command = 'qemu-img create -f qcow2 {0} {1}B'.format(
                    shlex.quote(img),
                    shlex.quote(str(size))
                )
                
                stderr = io.StringIO()
                
                self.__run_cmd(command)
            else:
                raise Exceptions.InvalidDiskFormat(
                    'No such format: {0}'.format(
                        DISK_FORMAT
                    )
                )
        else:
            raise Exceptions.DriveAlreadyExists(img)
        
    def create_snapshot(self, snapshot: str, backing: str):
        if os.path.isfile(snapshot):
            raise Exceptions.DriveAlreadyExists(snapshot)
        
        if not os.path.isfile(backing):
            raise Exceptions.LaunchDependencyMissing(backing)
            
        if DISK_FORMAT != 'qcow2':
            raise Exceptions.InvalidArgument(
                'Snapshots are only supported with qcow2.'
            )
            
        command = 'qemu-img create -f qcow2 -b {0} {1}'.format(
            shlex.quote(os.path.relpath(
                backing,
                os.path.dirname(snapshot)
            )),
            shlex.quote(snapshot)
        )
        
        self.__run_cmd(command)
        
    def list(self):
        """
        Returns a list of all the available virtual machine UUIDs.
        """
        
        return list(
            map(
                (lambda x: x[0]),
                self.db().query(self.VertiVM.id).all()
            )
        )
        
    def __run_cmd(self, command):
        stderrfile = tempfile.mkstemp()[1]
        stderr = open(stderrfile, 'w')
        
        subprocess.check_call(
            shlex.split(command),
            stdin  = subprocess.DEVNULL,
            stdout = subprocess.DEVNULL,
            stderr = stderr
        )
        stderr.close()
        stderr = open(stderrfile, 'r')
        
        stderr_read = stderr.read()
        os.remove(stderrfile)
        
        if len(stderr_read) > 0:
            raise Exceptions.SubprocessLaunchException(
                stderr_read
            )
        
    def __wrap_live(self, db_object):
        if EXPERIMENTAL_SHARED_INSTANCES:
            if not (db_object.id in self.vm_instances):
                self.vm_instances[db_object.id] = self.VertiVMLive(
                    self,
                    db_object.id
                )
            
            return self.vm_instances[db_object.id]
        else:
            return self.VertiVMLive(
                self,
                db_object.id
            )
    
    class VertiVMLive(object):
        class VMDisplay(object):
            class AudioOutput(object):
                def read(self):
                    """
                    Get the virtual machine's current audio buffer. 
                    
                    The output is a bytearray in WAVE format. If the buffer
                    is empty, the WAVE file will be 0 seconds long as it will
                    just contain the default 44 byte large WAVE/RIFF header and
                    nothing else.
                    
                    Every time you call this, the current contents of the audio
                    buffer will be erased. This means you can essentially just
                    construct a while loop to call this function over and over
                    again and play back the output through a PyAudio stream or
                    something. Just make sure you're playing it back fast
                    enough.
                    
                    If it takes too long for your audio playback system to
                    initialize the stream or whatever, your audio will stutter
                    a lot, and it won't sound pleasant at all. The only real
                    way around this is to have a constant stream that is only
                    initialized once, and to constantly feed it with this data
                    as fast as possible. To avoid ALSA buffer underruns, you
                    can feed the stream with pure silence whenever the VM's
                    audio grab stream returns an empty WAVE file.
                    
                    If there's a better way to do this, let me know - I have no
                    idea how any of this audio stuff works. Getting the test
                    script to work at an acceptable level of audio quality took
                    an entire day - honestly, making audio work with this
                    project was the one thing I dreaded the most.
                    """
                    
                    # The output will always be padded to the block size
                    ret = bytearray(AUDIO_BLOCK_SIZE)
                    
                    try:
                        chunk = self.audio.popleft()
                        
                        ret[:len(chunk)] = chunk
                    except IndexError:
                        pass
                        
                    # Erase any headers supplied by QEMU as they are not valid
                    # WAVE/RIFF headers.
                    if (ret[:4] == b'RIFF'
                        and ret[8:12] == b'WAVE'):
                        
                        ret = ret[44:]
                        
                    # Add our own headers instead, with properly calculated
                    # length values for each subchunk.
                    blank = bytearray(BLANK_WAV_HEADER)
                    
                    blank[4:8] = (
                        (len(blank) + len(ret)) - 8
                    ).to_bytes(4, byteorder='little')
                    
                    blank[40:44] = (
                        len(ret)
                    ).to_bytes(4, byteorder='little')
                    
                    ret = blank + ret
                        
                    return ret
                
                def close(self):
                    self.active = False
                
                def __audio_get_thread(self):
                    while self.active:
                        try:
                            context = zmq.Context()
                            sub = context.socket(zmq.SUB)
                            sub.setsockopt(zmq.SUBSCRIBE, b"")
                            sub.setsockopt(
                                zmq.RCVTIMEO, round(
                                    STATE_CHECK_NRLT_CLK_SECS * 1000
                                )
                            )
                            
                            self.vmlive.db_session().commit()
                            bindport = self.vmlive.db_object().ports['audio']
                            sub.connect('tcp://{0}:{1}'.format(
                                GLOBAL_LOOPBACK,
                                bindport
                            ))
                            
                            while self.active:
                                self.audio.append(sub.recv())
                                
                            sub.close()
                        except zmq.error.Again:
                            pass
                    self.vmlive.db_session().close()
                
                def __del__(self):
                    self.close()
                
                def __init__(self, display):
                    self.display = display
                    self.vmlive = self.display.vmlive
                    self.audio = collections.deque(
                        maxlen = round(AUDIO_MAX_SIZE / AUDIO_BLOCK_SIZE)
                    )
                    self.active = True
                    
                    self.threads = []
            
                    self.threads.append(
                        threading.Thread(
                            target = self.__audio_get_thread,
                            daemon = True
                        )
                    )
                    self.threads[-1].start()
            
            def disconnect(self):
                self.connected = False
                
                if self.client != None:
                    del self.client
                
                self.paste = self.__return_none
                self.mouseMove = self.__return_none
                self.mouseDown = self.__return_none
                self.mouseUp = self.__return_none
                self.keyDown = self.__return_none
                self.keyUp = self.__return_none
                
                self.client = None
            
            def refresh(self, force: bool = False):
                """
                Refresh the display. You don't need to call this either, it is
                already automatically called when you run capture().
                """
                    
                if not force:
                    if (time.time() - self.frame_lease) < (1 / VNC_FRAMERATE):
                        return
                
                try:
                    self.client.refreshScreen()
                except (TimeoutError, AttributeError, builtins.AttributeError):
                    self.disconnect()
                    
                self.frame_lease = time.time()
            
            def capture(self, force: bool = False):
                """
                Returns a PIL image of the virtual machine display. You can
                also interact with the VM through the following functions:
                    - paste()
                    - mouseMove()
                    - mouseDown()
                    - mouseUp()
                    - keyDown()
                    - keyUp()
                For an explanation of how they work, see the following...
                    - https://vncdotool.readthedocs.io/en/latest/modules.html
                    - https://vncdotool.readthedocs.io/en/latest/library.html
                """
                
                self.refresh(force = force)
                
                if self.client != None:
                    self.shape = self.client.screen.size
                    
                    return self.client.screen.convert(
                        VNC_IMAGE_MODE,
                        dither  = Image.NONE,
                        palette = Image.ADAPTIVE
                    )
                else:
                    offline_message = Image.new(VNC_IMAGE_MODE, self.shape)
                    ImageDraw.Draw(
                        offline_message
                    ).text(
                        (8, 8),
                        VNC_NO_SIGNAL_MESSAGE,
                        (255, 255, 0)
                    )
                    
                    self.connect()
                    
                    return offline_message
            
            def connect(self):
                # You do not need to call this, it is already called when a VM
                # starts or is detected to be online.
                
                self.disconnect()
                start_time = time.time()
                
                while (self.client == None
                        or (time.time() - start_time) > VNC_TIMEOUT_SECS
                    ) and (self.vmlive.state(
                        vnc_connecting = True
                    ) == 'online'):
                        
                    try:
                        self.client = vncapi.connect('{0}:{1}'.format(
                            GLOBAL_LOOPBACK,
                            (self.vmlive.db_object().ports['vnc']
                            - QEMU_VNC_ADDS)
                        ), password = None, timeout = VNC_TIMEOUT_SECS)
                        
                        self.paste = self.client.paste
                        self.mouseMove = self.client.mouseMove
                        self.mouseDown = self.client.mouseDown
                        self.mouseUp = self.client.mouseUp
                        self.keyDown = self.client.keyDown
                        self.keyUp = self.client.keyUp
                        
                        self.capture(force = True)
                        
                        self.connected = True
                    except (
                            vncapi.VNCDoException,
                            TimeoutError,
                            AttributeError,
                            builtins.AttributeError
                        ):
                        self.disconnect()
                        
            def getAudio(self):
                """
                Returns the VM's audio stream. Every time you read() it, the
                current audio buffer gets cleared, so if you intend to stream
                the audio to multiple places it's probably a good idea to use
                multiple of these. If you use just one instance and read it
                multiple times for different purposes, you'll end up with
                audio stuttering as parts of the buffer are randomly fired into
                different places and cleared or something. I don't know, audio
                is confusing and hard and I don't understand it myself really.
                
                The AudioOutput class has the following methods:
                    - read()
                    - close()
                    
                read() takes no arguments. It will give you the current buffer
                content in WAVE format.
                """
                return self.AudioOutput(self)
            
            def __return_none(self, *args, **kwargs):
                return None
                        
            def __init__(self, vmlive):
                self.vmlive = vmlive
                self.client = None
                self.connected = False
                self.disconnect()
                self.frame_lease = 0
                self.shape = (640, 480)
                
                self.connect()
                        
        def __init__(self, vertibird, vuuid):
            """
            Don't call this directly, but know that it assigns the VMDisplay
            object to self.display. This will allow you to interact with the VM
            """
            
            self._local     = threading.local()
            self.id         = vuuid
            self.vertibird  = vertibird
            self.db_session = vertibird.db
            self.db_object  = self.__get_db_object
            self.display    = self.VMDisplay(self)
            
            # State checking stuff
            self.state()
            
        def __del__(self):
            # Check state on exit/delete too, just in case.
            # (Also to ensure named pipe for audio is deleted)
            try:
                self.state()
            except:
                pass # Already garbage collected

        def __check_main_thread(self):
            return threading.main_thread().is_alive()

        def __audio_thread(self):
            """
            Automatically converts the named pipe into a ZMQ broadcast.
            """

            try:
                while self.state(vnc_connecting = True) == 'offline':
                    if not self.__check_main_thread():
                        return
                    
                    time.sleep(STATE_CHECK_CLK_SECS)
                    
                    self.db_session().commit()
                
                context = zmq.Context()
                socket = context.socket(zmq.PUB)
                
                self.db_session().commit()
                ftr = self.db_object().audiopipe
                bindport = self.db_object().ports['audio']
                
                try:
                    socket.bind('tcp://{0}:{1}'.format(
                        GLOBAL_LOOPBACK,
                        bindport
                    ))
                    
                    f = open(ftr, 'rb')
                except (zmq.error.ZMQError, FileNotFoundError):
                    return
                
                while self.state(vnc_connecting = True) != 'offline':
                    if not self.__check_main_thread():
                        break
                    
                    try:
                        if not os.path.exists(
                                ftr
                            ):
                            raise FileNotFoundError('Pipe missing')
                        
                        p = f.read(AUDIO_BLOCK_SIZE)
                        
                        socket.send(p)
                    except (FileNotFoundError, OSError):
                        break
                
                socket.close()
                f.close()
            except Exception as e:
                traceback.print_exc()
            
            self.db_object().audiothrd = False
            self.db_session().commit()
            
            self.db_session().close()

        def wait(self):
            """
            Waits until this VM has been terminated.
            """
            
            while self.state() != 'offline':
                time.sleep(STATE_CHECK_NRLT_CLK_SECS)
            
        def remove(self):
            """
            Removes the virtual machine from the database. Will NOT delete
            the storage or mounted ISOs. It is recommended to call
            Vertibird.remove() instead.
            """
            
            try:
                self.stop()
            except Exceptions.InvalidStateChange:
                pass # Already offline
            
            if self.display.connected:
                self.display.disconnect()
            
            self.db_session().delete(self.db_object())
            self.db_session().commit()
            
        def start(self):
            """
            Starts the virtual machine's QEMU process. Will raise an exception
            if the virtual machine is already running.
            """
            
            with GLOBAL_LOCK:
                self.__set_option_offline()
            
                self.__randomize_ports()
                
                # Remove audio pipe file just in case it wasn't cleared
                # automatically last time
                if self.db_object().audiopipe != None:
                    try:
                        os.unlink(self.db_object().audiopipe)
                    except OSError:
                        pass # Already removed
                
                self.db_object().audiopipe = self.__get_temp(
                    suffix = '.pipe'
                )
                
                self.db_session().commit()
                os.unlink(self.db_object().audiopipe)
                os.mkfifo(self.db_object().audiopipe)
                
                arguments = [
                    self.vertibird.qemu, # PROCESS
                    '-uuid',
                    self.__argescape(self.id),
                    '-monitor',
                    'telnet:{0}:{1},server,nowait'.format(
                        GLOBAL_LOOPBACK,
                        self.db_object().ports['monitor']
                    ),
                    '-nographic',
                    '-serial',
                    'none',
                    '-vnc',
                    '{0}:{1},share=force-shared'.format(
                        GLOBAL_LOOPBACK,
                        self.db_object().ports['vnc'] - QEMU_VNC_ADDS
                    ),
                    '-display',
                    'egl-headless', # Potentially allows 3D
                    '-m',
                    '{0}B'.format(self.db_object().memory),
                    '-overcommit',
                    'mem-lock=off',
                    '-boot',
                    'order={0},menu=on'.format(
                        self.__argescape(self.db_object().bootorder)
                    ),
                    '-cpu',
                    ('{0},-hypervisor').format(
                        self.__argescape(self.db_object().cpu)
                    ),
                    '-smp',
                    'cpus={3},sockets={0},cores={1},threads={2}'.format(
                        self.__argescape(str(self.db_object().sockets)),
                        self.__argescape(str(self.db_object().cores)),
                        self.__argescape(str(self.db_object().threads)),
                        self.__argescape(str(
                            self.db_object().sockets *
                            self.db_object().cores *
                            self.db_object().threads
                        ))
                    ),
                    '-machine',
                    'type={0},accel=kvm'.format(
                        self.__argescape(self.db_object().machine)
                    ),
                    '-enable-kvm',
                    '-sandbox',
                    ('on,obsolete=deny,elevateprivileges=deny,spawn=deny,'+
                    'resourcecontrol=deny'),
                    '-rtc',
                    'base={0},clock=host,driftfix=slew'.format(
                        self.__argescape(self.db_object().rtc)
                    ),
                    '-device',
                    # I had to remove escaping for the VGA device because
                    # some of them have optional properties (like ati-vga
                    # and virtio-gpu) that allow for important features
                    # that un-privileged users should have access to.
                    # Luckily this doesn't really matter (unless the SQLite
                    # database is compromised) because the name of the VGA
                    # device is checked against the list of available VGA
                    # devices and variations when you set it anyway, and it
                    # will raise an exception if the device is invalid.
                    self.db_object().vga,
                    '-audiodev',
                    'wav,path={0},id=audioout'.format(
                        self.__argescape(self.db_object().audiopipe)
                    ),
                    '-device',
                    '{0},netdev=net0'.format(
                        self.__argescape(self.db_object().network)
                    ),
                    '-netdev',
                    'user,id=net0{0}{1}'.format(
                        (lambda x: ',' if x > 0 else '')(
                            len(self.db_object().forwarding)
                        ),
                        ','.join([
                            'hostfwd={0}:{1}:{2}-:{3}'.format(
                                self.__argescape(x['protocol']),
                                self.__argescape(x['external_ip']),
                                self.__argescape(x['external_port']),
                                self.__argescape(x['internal_port'])
                            ) for x in self.db_object().forwarding
                        ])
                    ),
                    '-smbios',
                    ('type=0,vendor="American Megatrends Inc.",version=' +
                    'B.40,date=11/07/2019,release=5.14,uefi=off'),
                    '-smbios',
                    ('type=1,manufacturer="Gigabyte Technology Co. Ltd"' +
                    ',product=MS-7A38,uuid={0},version=8.0,' +
                    'serial={1}').format(
                        self.__argescape(self.id),
                        self.__argescape('To be filled by O.E.M.')
                    )
                ]
                
                if self.db_object().machine != 'isapc':
                    arguments += [
                        '-device',
                        '{0},id=usb'.format(
                            {
                                'pc': 'piix3-usb-uhci',
                                'q35': 'ich9-usb-uhci1'
                            }[self.db_object().machine]
                        ),
                        '-device',
                        '{0},id=scsi'.format(
                            self.__argescape(self.db_object().scsi)
                        ),
                        '-device',
                        '{0},id=ahci'.format(
                            {
                                'pc': 'ahci',
                                'q35': 'ich9-ahci'
                            }[self.db_object().machine]
                        ),
                        '-object',
                        'rng-random,id=rng0,filename=/dev/urandom',
                        '-device',
                        'virtio-rng-pci,rng=rng0'
                    ]
                    
                    if self.db_object().inputdev in [
                            'usb-mouse', 'usb-tablet'
                        ]:
                        arguments += [
                            '-device',
                            '{0},id=input0'.format(
                                self.db_object().inputdev
                            ),
                            '-device',
                            'usb-kbd,id=input1'
                        ]
                else:
                    if not (self.db_object().sound in [
                            'adlib','sb16','gus'
                        ]):
                        raise Exceptions.InvalidGenericDeviceType(
                            'ISA-Only PC requires an ISA audio device'
                        )
                    elif not (self.db_object().network in ['ne2k_isa']):
                        raise Exceptions.InvalidGenericDeviceType(
                            'ISA-Only PC requires an ISA NIC device'
                        )
                    elif not ('isa' in self.db_object().vga):
                        raise Exceptions.InvalidGenericDeviceType(
                            'ISA-Only PC requires an ISA VGA device'
                        )
                    elif self.db_object().cores > 1:
                        raise Exceptions.InvalidGenericDeviceType(
                            'ISA-Only PC can only support 1 core'
                        )
                
                if self.db_object().floppy != None:
                    if not (os.path.isfile(self.db_object().floppy)):
                        raise Exceptions.LaunchDependencyMissing(
                            self.db_object().floppy
                        )
                    else:
                        arguments += [
                            '-fda',
                            self.__argescape(self.db_object().floppy)
                        ]
                        
                if self.db_object().numa == True:
                    arguments.append('-numa')
                
                if self.db_object().sound in [
                        'ac97', 'adlib', 'sb16', 'gus'
                    ]:
                    arguments += [
                        '-device',
                        '{0},audiodev=audioout'.format(
                            (lambda x: x.upper() if x == 'ac97' else x)(
                                self.db_object().sound
                            )
                        )
                    ]
                elif self.db_object().sound == 'hda':
                    arguments += [
                        '-device',
                        '{0},id=hda'.format(
                            {
                                'pc': 'intel-hda',
                                'q35': 'ich9-intel-hda'
                            }[self.db_object().machine]
                        ),
                        '-device',
                        'hda-output,id=hda-codec,audiodev=audioout'
                    ]
                else:
                    raise Exceptions.InvalidGenericDeviceType(
                        'Audio device type is invalid!'
                    )
                
                strdevices = 0
                
                for key, cdrom in enumerate(self.db_object().cdroms):
                    if os.path.isfile(cdrom):
                        internal_id = self.__random_device_id()
                        
                        arguments += [
                            '-drive',
                            'id={0},file={1},if=none,media=cdrom'.format(
                                internal_id,
                                self.__argescape(os.path.abspath(cdrom))
                            ),
                            '-device',
                            'ide-cd,drive={0},bus=ide.0,unit={1}'.format(
                                internal_id,
                                strdevices
                            )
                        ]
                        strdevices += 1
                    else:
                        raise Exceptions.LaunchDependencyMissing(cdrom)
                        
                for key, drive in enumerate(self.db_object().drives):
                    internal_id = self.__random_device_id()
                    
                    if os.path.isfile(drive['path']):
                        if (
                                self.db_object().machine == 'isapc'
                                and drive['type'] != 'ide'
                            ):
                            raise Exceptions.InvalidGenericDeviceType(
                                'Only IDE is supported on ISA-Only PC'
                            )
                        
                        if drive['type'] in ['ahci', 'ide', 'scsi']:
                            arguments += [
                                '-drive',
                                ('id={0},file={1},if=none,' +
                                'format={2}').format(
                                    internal_id,
                                    self.__argescape(
                                        os.path.abspath(drive['path'])
                                    ),
                                    DISK_FORMAT
                                ),
                                '-device',
                                {
                                    'ahci': '{3},drive={0},bus={1}.0',
                                    'ide': ('{3},drive={0},bus={1}.0' +
                                    ',unit={2}'),
                                    'scsi': '{3},drive={0},bus={1}.0'
                                }[drive['type']].format(
                                    internal_id,
                                    self.__argescape(drive['type']),
                                    strdevices,
                                    {
                                        'ahci': 'ide-hd',
                                        'ide': 'ide-hd',
                                        'scsi': 'scsi-hd'
                                    }[drive['type']]
                                )
                            ]
                            if drive['type'] == 'ide':
                                strdevices += 1
                        elif drive['type'] == 'virtio':
                            arguments += [
                                '-drive',
                                ('id={0},file={1},if=virtio' + 
                                ',format={2}').format(
                                    internal_id,
                                    self.__argescape(
                                        os.path.abspath(drive['path'])
                                    ),
                                    DISK_FORMAT
                                )
                            ]
                        else:
                            raise Exceptions.InvalidGenericDeviceType(
                                'Invalid storage device type.'
                            )
                    else:
                        raise Exceptions.LaunchDependencyMissing(
                            drive['path']
                        )
                
                if strdevices > 2:
                    raise Exceptions.LimitReached(
                        'IDE only supports a maximum of 2 units.'
                    )
                
                if DEBUG:
                    arguments.remove('-nographic')
                    arguments += [
                        '-display',
                        'gtk'
                    ]
                
                logfile = self.__get_fresh_log_file()
                logfile_handle = open(logfile, 'a')
                
                # VM LAUNCH
                process = subprocess.Popen(
                    arguments,
                    stderr = logfile_handle,
                    stdout = logfile_handle
                )
                
                pid = process.pid
                
                self.db_object().pid   = pid
                self.db_object().state = 'online'
                self.db_session().commit()
                
                self.state()
                self.display.connect()
                
                time.sleep(STATE_CHECK_NRLT_CLK_SECS)
                
                if not (process.returncode in [None, 0]):
                    self.state()
                    
                    raise Exceptions.VMLaunchException(
                        'The virtual machine was unable to launch. ' +
                        'Check the log for more information.'
                    )
        
        def get_log(self):
            """
            Gets the log file's file object
            """
            try:
                return open(self.db_object().log, 'r')
            except FileNotFoundError:
                return open(self.__get_fresh_log_file(), 'r')
        
        def get_properties(self):
            """
            Returns a dictionary containing the properties for the virtual
            machine, such as the memory, core count, CPU model, machine model
            and graphics adapter model.
            """
            return {
                'memory'    : self.db_object().memory   ,
                'cores'     : self.db_object().cores    ,
                'cpu'       : self.db_object().cpu      ,
                'machine'   : self.db_object().machine  ,
                'vga'       : self.db_object().vga      ,
                'sound'     : self.db_object().sound    ,
                'bootorder' : self.db_object().bootorder,
                'network'   : self.db_object().network  ,
                'floppy'    : self.db_object().floppy   ,
                'numa'      : self.db_object().numa     ,
                'scsi'      : self.db_object().scsi     ,
                'rtc'       : self.db_object().rtc      ,
                'inputdev'  : self.db_object().inputdev ,
            }
        
        def set_properties(self, properties: dict):
            """
            Replaces all of the virtual machine options with the contents of a
            property dictionary, of which can be obtained with get_properties()
            """
            self.__set_option_offline()
            
            memory    = int (properties['memory'   ])
            sockets   = int (properties['sockets'  ])
            cores     = int (properties['cores'    ])
            threads   = int (properties['threads'  ])
            cpu       = str (properties['cpu'      ])
            machine   = str (properties['machine'  ])
            vga       = str (properties['vga'      ])
            sound     = str (properties['sound'    ])
            bootorder = str (properties['bootorder'])
            network   = str (properties['network'  ])
            floppy    =     (properties['floppy'   ])
            scsi      = str (properties['scsi'     ])
            numa      = bool(properties['numa'     ])
            inputdev  = str (properties['inputdev' ])
            rtc       = str (properties['rtc'      ])
            
            totcores = sockets * cores * threads
            
            # So apparently assertions can be removed in production use or
            # whatever, so I have to do this horrible mess instead. Is this
            # really what you wanted? Do you have any idea how much better a
            # bunch of assertions would've looked compared to THIS?! Who the
            # hell thought assertions ought to be expendable...
            if memory < 8388608:
                raise Exceptions.InvalidArgument('Memory allocation too low')
            elif totcores > os.cpu_count() or totcores < 1:
                raise Exceptions.InvalidArgument('Invalid core count')
            elif not (vga in QEMUDevices.vga.keys()):
                raise Exceptions.InvalidArgument('Invalid display adapter')
            elif not (machine in QEMUDevices.machine.keys()):
                raise Exceptions.InvalidArgument('Invalid machine type')
            elif not (sound in QEMUDevices.sound.keys()):
                raise Exceptions.InvalidArgument('Invalid audio adapter type')
            elif not (network in QEMUDevices.network.keys()):
                raise Exceptions.InvalidArgument('Invalid network device type')
            elif not (scsi in QEMUDevices.scsi.keys()):
                raise Exceptions.InvalidArgument('Invalid SCSI controller')
            elif (not set('abcdnp').issuperset(bootorder)):
                raise Exceptions.InvalidArgument('Invalid boot order')
            elif (floppy != None):
                if (not os.path.isfile(floppy)) or (type(floppy) != str):
                    raise Exceptions.InvalidArgument('Invalid floppy file')
            elif not (rtc in QEMUDevices.rtc.keys()):
                raise Exceptions.InvalidArgument('Invalid RTC clock preset')
            elif not (cpu in QEMUDevices.cpu.keys()):
                raise Exceptions.InvalidArgument('Invalid processor')
            elif not (inputdev in QEMUDevices.inputs.keys()):
                raise Exceptions.InvalidArgument('Invalid input device')
            
            self.db_object().memory    = memory
            self.db_object().sockets   = sockets
            self.db_object().cores     = cores
            self.db_object().threads   = threads
            self.db_object().cpu       = cpu
            self.db_object().machine   = machine
            self.db_object().vga       = vga
            self.db_object().sound     = sound
            self.db_object().bootorder = bootorder
            self.db_object().network   = network
            self.db_object().floppy    = floppy
            self.db_object().scsi      = scsi
            self.db_object().numa      = numa
            self.db_object().inputdev  = inputdev
            self.db_object().rtc       = rtc
            self.db_session().commit()
        
        def forward_port(self,
                external_port,
                internal_port,
                protocol: str = 'tcp',
                external_ip: str = '0.0.0.0'
            ) -> str:
            """
            Forwards a port from within the VM to a port outside of it.
            Returns the forwarding ID.
            """
            
            self.__set_option_offline()
            
            protocol = protocol.lower()
            
            if not self.__validate_ip(external_ip):
                raise Exceptions.InvalidArgument('Invalid external IP')
            elif not self.__validate_port(external_port):
                raise Exceptions.InvalidArgument('Invalid external port')
            elif not self.__validate_port(internal_port):
                raise Exceptions.InvalidArgument('Invalid internal port')
            elif not (protocol in ['tcp', 'udp']):
                raise Exceptions.InvalidArgument(
                    'Protocol must be tcp or udp.'
                )
            
            fwd_id = str(zlib.crc32(('-'.join([
                protocol,
                external_ip,
                str(external_port),
                str(internal_port)
            ])).encode()))
            
            if not (fwd_id in list(map(
                    (lambda x: x['id']),
                    self.db_object().forwarding
                ))):
                    
                self.db_object().forwarding = (self.db_object().forwarding + [
                    {
                        'id': fwd_id,
                        'protocol': protocol,
                        'external_ip': external_ip,
                        'external_port': str(external_port),
                        'internal_port': str(internal_port)
                    },
                ])
                self.db_session().commit()
            
            return fwd_id
            
        def list_forwardings(self):
            """
            Returns a list containing dictionaries, all of which are different
            port forwards specified for this VM.
            """
            
            return self.db_object().forwarding
            
        def remove_forwarding(self, fwd_id: str):
            """
            Remove a port forward based on forward ID.
            """
            
            self.__set_option_offline()
            
            self.db_object().forwarding = list(filter(
                lambda x: x['id'] != fwd_id,
                self.db_object().forwarding
            ))
            self.db_session().commit()
        
        def attach_cdrom(self, iso: str):
            """
            Attaches a path to a CD-ROM iso to the virtual machine.
            """
            
            self.__set_option_offline()
            
            if not os.path.isfile(iso):
                raise Exceptions.LaunchDependencyMissing(iso)
            
            if not (iso in self.db_object().cdroms):
                # Weird appending is required to trigger dirty state
                self.db_object().cdroms = (self.db_object().cdroms + [iso,])
                self.db_session().commit()
                
        def list_cdroms(self):
            """
            Returns a list containing strings, all of which are paths to the
            ISOs attached to the VM as CD-ROM drives.
            """
            
            return self.db_object().cdroms
            
        def detach_cdrom(self, iso: str):
            """
            Detaches a given CD-ROM iso path from the virtual machine.
            """
            
            self.__set_option_offline()
            
            if (iso in self.db_object().cdroms):
                self.db_object().cdroms = list(filter(
                    lambda x: x != iso,
                    self.db_object().cdroms
                ))
                self.db_session().commit()
                
        def attach_drive(self, img: str, dtype: str = 'ide'):
            """
            Attaches a drive to the virtual machine.
            
            Parameters
            ----------
            img :
                The relative OR absolute path of the image. This will be used
                to identify the image.
            dtype :
                The interface type for the drive.
                Can be IDE, AHCI, SCSI or VirtIO.
            """
            
            self.__set_option_offline()
            
            dtype = dtype.lower()
            if not (dtype in QEMUDevices.storage.keys()):
                raise Exceptions.InvalidDriveType(
                    'No such type {0}.'.format(dtype)
                )
                
            if not os.path.isfile(img):
                raise Exceptions.LaunchDependencyMissing(img)
                
            if not (img in list(map(
                    lambda x: x['path'],
                    self.db_object().drives
                ))):
                    
                self.db_object().drives = self.db_object().drives + [{
                    'path': img,
                    'type': dtype
                },]
                
                self.db_session().commit()
            
        def list_drives(self):
            """
            Returns a list of dictionaries specifying the path and interface
            type of each drive attached to the virtual machine.
            """
            
            return self.db_object().drives
            
        def detach_drive(self, img: str):
            """
            Detaches a given drive path from the VM.
            """
            
            self.__set_option_offline()
            
            self.db_object().drives = list(filter(
                lambda x: x['path'] != img,
                self.db_object().drives
            ))
            self.db_session().commit()
            
        def create_or_attach_drive(
                self, img: str, size: int = DEFAULT_DSIZE, dtype: str = 'ide'
            ):
            """
            Create a drive image if it doesn't exist and attach it if it isn't
            already attached. See Vertibird.create_drive() and
            Vertibird.VertiVMLive.attach_drive() for argument explanations.
            """
                
            self.__set_option_offline()
            
            if not os.path.isfile(img):
                self.vertibird.create_drive(img, size)

            self.attach_drive(img, dtype)
        
        def get_cpu_percent(self):
            """
            Return the percentage CPU utilization for the virtual machine.
            
            It is automatically divided between the cores, so a virtual machine
            with 2 cores running a dual-core benchmark would report a
            utilization of 100% and a completely idle virtual machine with 2
            cores would report 0%. Values can potentially be larger than 100%.
            """
            
            self.__check_running()
            
            return psutil.Process(
                self.db_object().pid
            ).cpu_percent(interval=CPU_USAGE_INTERVAL) / self.db_object().cores
            
        def get_memory_usage(self):
            """
            Return the resident set size (non-swapped physical memory) that
            has been consumed by the virtual machine
            """
            self.__check_running()
            
            return psutil.Process(
                self.db_object().pid
            ).memory_info().rss
        
        def signal_shutdown(self):
            """
            Sends the shutdown signal. This is non-blocking.
            """
            
            self.__check_running()
            
            self.__send_monitor_command('system_powerdown')
            
        def signal_reset(self):
            """
            Sends the reset signal. This is non-blocking.
            """
            
            self.__check_running()
            
            self.__send_monitor_command('system_reset')
            
        def stop(self):
            """
            Terminates the virtual machine.
            """
            
            with GLOBAL_LOCK:
                self.__check_running()
                
                try:
                    self.__send_monitor_command('quit')
                except:
                    pass # Could not have initialized yet
                
                try:
                    os.kill(self.db_object().pid, signal.SIGINT)
                except ProcessLookupError:
                    pass # Process does not exist
                
                self.state()
            
        def state(self, vnc_connecting: bool = False) -> str:
            """
            Return the current state of this virtual machine.
            
            Returns
            -------
            state :
                The current VM state, can be "offline" or "online"
            """
            
            self._state_check(vnc_connecting)
            
            return self.db_object().state
            
        def _state_check(self, vnc_connecting: bool = False):
            # Check if QEMU instance is actually still running
            try:
                pid = self.db_object().pid
                x = psutil.Process(pid)
                
                # Process ID may have been reclaimed
                if not (self.id in x.cmdline()):
                    raise psutil.NoSuchProcess(pid)
                
                # Process may not immediately end
                if x.status() == 'zombie':
                    try:
                        x.kill()
                    except:
                        pass
                    
                    raise psutil.NoSuchProcess(pid)
            except psutil.NoSuchProcess:
                pass
            else:
                if not vnc_connecting:
                    if self.display.connected == False:
                        self.display.connect()
                    
                if self.db_object().audiothrd == False:
                    self.db_object().audiothrd = True
                    self.db_session().commit()
                    
                    threading.Thread(
                        target = self.__audio_thread,
                        daemon = False
                    ).start()
                
                return
            
            self.__mark_offline()
            
        def __mark_offline(self):
            self.__file_cleanup()
            
            self.db_object().handles = None
            self.db_object().audiothrd = False
            self.db_object().pid = None
            self.db_object().state = 'offline'
            self.db_session().commit()
            
            try:
                self.display.disconnect()
            except AttributeError:
                pass # Display not set up yet
            
        def __file_cleanup(self):
            try:
                os.unlink(self.db_object().audiopipe)
            except (FileNotFoundError, TypeError, OSError):
                pass # Already removed by something, perhaps a reboot
            else:
                self.db_object().audiopipe = None
                self.db_session().commit()
            
        def __argescape(self, i: str):
            if any((c in set(',=!?<>~#@:;$*()[]{}&%"\'\\+')) for c in i):
                raise Exceptions.InvalidArgument(
                    ('Attempted to supply a malformed argument to QEMU! ' +
                     'String was: {0}'.format(i))
                )
            
            return shlex.quote(i)
            
        def __validate_ip(self, i: str):
            try:
                ipaddress.ip_address(i)
            except ValueError:
                return False
            else:
                return True
            
        def __validate_port(self, i):
            if '.' in str(i):
                return False
            
            try:
                i = int(i)
            except:
                return False
                
            if i > 65535:
                return False
            elif i < 1:
                return False
            else:
                return True
            
        def __send_monitor_command(self, command: str):
            with telnetlib.Telnet(
                GLOBAL_LOOPBACK,
                self.db_object().ports['monitor'],
                TELNET_TIMEOUT_SECS
            ) as session:
                session.read_until(b'(qemu) ')
                session.write(
                    '{0}'.format(command).encode('ascii') + b'\n'
                )
                session.read_until(b'(qemu) ')
                session.close()
            
        def __get_fresh_log_file(self):            
            if self.db_object().log == None:
                self.db_object().log = self.__get_temp()
            elif not os.path.isfile(self.db_object().log):
                self.db_object().log = self.__get_temp()
            elif os.path.getsize(self.db_object().log) > MAX_LOG_SIZE:
                os.unlink(self.db_object().log)
                self.db_object().log = self.__get_temp()
                
            self.db_session().commit()

            return self.db_object().log
            
        def __get_temp(self, suffix: str = '.dat') -> str:
            generated = tempfile.mkstemp(
                suffix = suffix
            )
            
            # Nobody is going to use the OS-level handle
            os.close(generated[0])
            
            return generated[1]
            
        def __randomize_ports(self):
            for x in self.db_object().ports.values():
                if not self.vertibird._check_port_open(x):
                    self.db_object().ports = self.vertibird._new_ports()
                    self.db_session().commit()
                    
                    break
            
        def __random_device_id(self, length: int = 16):
            return ''.join(
                [random.choice(string.ascii_lowercase) for _ in range(length)]
            )
            
        def __set_option_offline(self):
            if self.state() != 'offline':
                raise Exceptions.InvalidStateChange(
                    'Function requires VM to be offline'
                )
                
        def __check_running(self):
            if self.state() == 'offline':
                raise Exceptions.InvalidStateChange(
                    'Function requires VM to be online'
                )
                
        def __get_new_db_object(self):
            self._local.dbobj = {
                'object': self.db_session().query(
                    self.vertibird.VertiVM
                ).filter(
                    self.vertibird.VertiVM.id == self.id
                ).one(),
                'session': self.db_session(),
                'lease': time.time()
            }
                
        def __get_db_object(self):
            if not hasattr(self._local, 'dbobj'):
                self.__get_new_db_object()
                
            if ((not self._local.dbobj['session'].is_active)
                or self._local.dbobj['session'].connection().closed
                or self._local.dbobj['session'].deleted):
                    
                self._local.dbobj['session'].close()
                self.__get_new_db_object()
                
            if (time.time() - self._local.dbobj['lease']) > DB_CINTERVAL:
                self._local.dbobj['session'].commit()
                self._local.dbobj['lease'] = time.time()
                
            return self._local.dbobj['object']
    
    class VertiVM(Base):
        """
        Internal database class, allows for the definition of virtual machines
        via SQLAlchemy.
        """
        
        __tablename__ = 'machines'
        
        id         = Column(String, primary_key=True)
        ports      = Column(PickleType)
        log        = Column(String)
        pid        = Column(Integer)
        audiopipe  = Column(String)
        audiothrd  = Column(Boolean, default = False)
        state      = Column(String, default = 'offline')
        memory     = Column(Integer, default = 134217728)
        sockets    = Column(Integer, default = 1)
        cores      = Column(Integer, default = 1)
        threads    = Column(Integer, default = 1)
        cpu        = Column(String, default = 'host')
        machine    = Column(String, default = 'pc')
        vga        = Column(String, default = 'VGA')
        sound      = Column(String, default = 'hda')
        bootorder  = Column(String, default = 'cdn')
        network    = Column(String, default = 'rtl8139')
        scsi       = Column(String, default = 'lsi53c895a')
        rtc        = Column(String, default = 'utc')
        floppy     = Column(String)
        inputdev   = Column(String, default = 'ps2')
        numa       = Column(Boolean, default = False)
        cdroms     = Column(PickleType, default = [])
        drives     = Column(PickleType, default = [])
        forwarding = Column(PickleType, default = [])
    
    def __find_free_port(self) -> int:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]
    
    def _check_port_open(self, port) -> bool:
        host = GLOBAL_LOOPBACK
        
        with closing(
                socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ) as sock:
                
            if sock.connect_ex((host, port)) == 0:
                return True
            else:
                return False
    
    def _check_thread_id_alive(self, thread_id: int) -> bool:
        if thread_id in map(lambda x: x.ident, threading.enumerate()):
            return True
        else:
            return False
    
    def _new_ports(self) -> dict:
        return {
            'vnc': self.__find_free_port(),
            'monitor': self.__find_free_port(),
            'audio': self.__find_free_port()
        }
        
def session_generator(*args, **kwargs):
    return (lambda: Vertibird(*args, **kwargs))
        
class SessionManager(object):
    """
    THIS CLASS IS DEPRECATED, DO NOT USE IT.
    Vertibird() is now threadsafe. Use that instead!
    
    DEPRECATED DESCRIPTION
    This allows you to create thread-local or function-local Vertibird
    instances. The reason for this is because Vertibird instances are NOT
    thread-safe, but it IS safe to use multiple instances concurrently, even
    to access the same virtual machine.
    
    In summary...
    ---------------------------------------------------------------------------
    THREAD SAFE         : ~~NO~~ [THIS IS NOW YES]
    MULTI-INSTANCE SAFE : YES
    PROCESS SAFE        : YES
    """
    
    def __init__(self, *args, **kwargs):
        """
        Takes exactly the same arguments as Vertibird(), except it creates a
        decorator that automatically spawns Vertibird sessions for you.
        """
        
        self.vargs = args
        self.vkwargs = kwargs
        self.instances = {}
    
    def __make(self):
        thread = threading.get_ident()
        
        if not (thread in self.instances):
            self.instances[thread] = session_generator(
                *self.vargs,
                **self.vkwargs
            )()
            
        return self.instances[thread]
    
    def local(self):
        """
        Return a thread-local instance of Vertibird. Can be used like this...
        
        x = VertibirdSpawner()
        virtualmachine = x.local().get('uuid-uuid-uuid-uuid')
        """
        return self.__make()
    
    def vsession(self, func):
        """
        Decorator for spawning vertibird sessions. Creates the keyword
        argument "vertibird" for you. You will need to allow the definition of
        this argument when you define your function. It won't matter if you
        give it a default value.
        """
        
        def wrapper(*args, **kwargs):
            kwargs['vertibird'] = self.__make()
            
            return func(*args, **kwargs)
            
        return wrapper
        
if __name__ == '__main__':
    import cv2
    import numpy as np
    import pyaudio
    import wave
    import queue
    
    vspawner = SessionManager()
    local = vspawner.local

    def main():
        global DEBUG
        DEBUG = True
        global DISK_FORMAT
        DISK_FORMAT = 'qcow2'
        
        x = local()
        
        if len(x.list()) < 1:
            y = x.create()
        else:
            y = x.get(x.list()[-1])
        
        if y.state() == 'offline':
            for dsk in y.list_cdroms():
                y.detach_cdrom(dsk)
            for dsk in y.list_drives():
                y.detach_drive(dsk['path'])
            for fwd in y.list_forwardings():
                y.remove_forwarding(fwd['id'])
            
            #y.attach_cdrom(
            #    '/home/naphtha/Downloads/ubuntu-20.04-desktop-amd64.iso'
            #)
            
            dsize = 34359738368
            drives = './drives/'
            backing = os.path.join(drives, 'win10.qcow2')
            
            try:
                x.create_drive(
                    backing,
                    dsize
                )
            except Exceptions.DriveAlreadyExists:
                pass
            
            """
            try:
                x.create_snapshot(
                    snapshot,
                    backing,
                    dsize - os.path.getsize(backing)
                )
            except Exceptions.DriveAlreadyExists:
                pass
            """
            
            y.attach_drive(
                backing,
                'ahci'
            )
            
            options = y.get_properties()
            options['machine'] = 'pc'
            options['memory'] = 8589934592
            options['cpu'] = 'EPYC-IBPB'
            options['sockets'] = 1
            options['cores'] = 6
            options['threads'] = 2
            options['network'] = 'e1000'
            options['sound'] = 'hda'
            options['vga'] = 'vmware-svga'
            options['scsi'] = 'lsi53c895a'
            options['floppy'] = None
            options['inputdev'] = 'usb-mouse'
            y.set_properties(options)
                        
        try:
            # This tests if multi-processing is alright
            Vertibird().get(y.id).start()
        except Exceptions.InvalidStateChange:
            print('VM already running')
        
        print('Start non-blocking')
        
        imgGet = (lambda: cv2.cvtColor(np.asarray(
            y.display.capture().convert('RGB')
        ), cv2.COLOR_RGB2BGR))
        
        def audplay(y):
            p = pyaudio.PyAudio()
            stream = p.open(format=8,
                            channels=2,
                            rate=44100,
                            output=True)
            aud = y.display.getAudio()
            
            while y.state():
                grab = aud.read()
                
                wf = wave.open(io.BytesIO(grab))
                
                stream.write(wf.readframes(wf.getnframes()))
                
                wf.close()
                        
            stream.stop_stream()
            stream.close()
            aud.close()
            
        def logplay(y):
            logfile = y.get_log()
            while True:
                line = logfile.readline()
                if not line:
                    time.sleep(STATE_CHECK_CLK_SECS)
                    continue
                sys.stdout.write(line)
                
        def testthread(y):
            print(y.state())

        threading.Thread(
            target = audplay, args = (y,), daemon = True
        ).start()
        
        threading.Thread(
            target = logplay, args = (y,), daemon = True
        ).start()
        
        threading.Thread(
            target = testthread, args = (y,), daemon = True
        ).start()
        
        time.sleep(1)
        
        """
        while y.state() == 'online':
            z = imgGet()
                
            cv2.imshow('image', z)
            cv2.waitKey(34)
            
            #i = (lambda i: 'None' if bool(i) == False else i)(input('>>> '))
            #print(eval(i))
        """
        
        y.wait()
        
        if y.state() == 'online':
            y.stop()
            
    main()
