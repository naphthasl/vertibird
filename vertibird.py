import shelve, threading, time, uuid, socket, subprocess, os, telnetlib, signal
import sys, shlex, random, string, psutil

from contextlib import closing

from vncdotool import api as vncapi
from PIL import Image

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, PickleType, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.declarative import declarative_base

GLOBAL_LOOPBACK = '127.0.0.1'
QEMU_VNC_ADDS = 5900
TELNET_TIMEOUT_SECS = 1
VNC_TIMEOUT_SECS = TELNET_TIMEOUT_SECS
STATE_CHECK_CLK_SECS = 0.05
DEFAULT_DSIZE = 8589934592
VNC_IMAGE_MODE = 'RGB'

class Vertibird(object):
    class IncompatibleOperatingSystem(Exception):
        pass
    
    Base = declarative_base()

    def __init__(
                self,
                qemu: str = 'qemu-kvm',
                persistence: str = 'sqlite:///vertibird.db'
            ):
        """
        Create a Vertibird hypervisor interface.
        
        Parameters
        ----------
        qemu :
            The QEMU executable to use, defaults to qemu-kvm
        persistence:
            The shelf/database to store information about created VMs in,
            defaults to sqlite:///vertibird.db
        
        Raises
        ------
        """
        
        # Not sure if this is a good idea
        if not ('linux' in sys.platform.lower()):
            raise IncompatibleOperatingSystem('Only Linux is supported.')
        
        self.qemu = qemu
        self.engine = create_engine(persistence, strategy='threadlocal')
        self.Base.metadata.create_all(self.engine)
        self.db = scoped_session((sessionmaker(bind = self.engine)))()
        
        for x in self.list():
            self.get(x)._state_check()
    
    def create(self):
        x = self.VertiVM(id = str(uuid.uuid4()), ports = self._new_ports())
        self.db.add(x)
        self.db.commit()
        
        return self.__wrap_live(x)
        
    def get(self, vmuuid = str):
        return self.__wrap_live(self.db.query(self.VertiVM).filter(
            self.VertiVM.id == vmuuid
        ).one())
    
    def remove(self, vmuuid = str):
        self.get(vmuuid).remove()
        
    def create_drive(self, img: str, size: int = DEFAULT_DSIZE):
        if not os.path.isfile(img):
            f = open(img, 'wb')
            f.truncate(size)
            f.close()
        else:
            raise self.DriveAlreadyExists(img)
        
    def list(self):
        return list(
            map(
                (lambda x: x[0]),
                self.db.query(self.VertiVM.id).all()
            )
        )
        
    def __wrap_live(self, db_object):
        return self.VertiVMLive(self, self.db, db_object)
    
    class DriveAlreadyExists(Exception):
        pass
    
    class VertiVMLive(object):
        class InvalidStateChange(Exception):
            pass
            
        class LaunchDependencyMissing(Exception):
            pass
        
        class InvalidDriveType(Exception):
            pass
            
        class VMDisplay(object):
            def disconnect(self):
                if self.client != None:
                    try:
                        self.client.disconnect()
                    except:
                        pass
                
                self.paste = self.__return_none
                self.mouseMove = self.__return_none
                self.mouseDown = self.__return_none
                self.mouseUp = self.__return_none
                self.keyDown = self.__return_none
                self.keyUp = self.__return_none
                
                self.client = None
            
            def capture(self):
                try:
                    self.client.refreshScreen()
                except (TimeoutError, AttributeError):
                    self.disconnect()
                
                if self.client != None:
                    self.shape = self.client.screen.size
                    
                    return self.client.screen.convert(
                        VNC_IMAGE_MODE,
                        dither  = Image.NONE,
                        palette = Image.ADAPTIVE
                    )
                else:
                    return Image.new(VNC_IMAGE_MODE, self.shape)
            
            def connect(self):
                if self.vmlive.state() == 'online':
                    start_time = time.time()
                    
                    while (self.client == None
                            or (time.time() - start_time) > VNC_TIMEOUT_SECS
                        ):
                            
                        try:
                            self.client = vncapi.connect('{0}:{1}'.format(
                                GLOBAL_LOOPBACK,
                                (self.vmlive.db_object.ports['vnc']
                                - QEMU_VNC_ADDS)
                            ), password = None, timeout = VNC_TIMEOUT_SECS)
                            
                            self.paste = self.client.paste
                            self.mouseMove = self.client.mouseMove
                            self.mouseDown = self.client.mouseDown
                            self.mouseUp = self.client.mouseUp
                            self.keyDown = self.client.keyDown
                            self.keyUp = self.client.keyUp
                            
                            self.capture()
                        except (vncapi.VNCDoException, TimeoutError):
                            self.disconnect()
                else:
                    self.disconnect()
            
            def __wrap(self, func):
                def wrapper(self, *args, **kwargs):
                    if self.client == None:
                        return self.__return_none()
                    else:
                        return func(self.client, *args, **kwargs)
                    
                return wrapper
            
            def __return_none(self, *args, **kwargs):
                return None
            
            def __init__(self, vmlive):
                self.vmlive = vmlive
                self.client = None
                self.disconnect()
                self.shape = (640, 480)
                
                self.connect()
                        
        def __init__(self, vertibird, db_session, db_object):
            self.vertibird  = vertibird
            self.db_session = db_session
            self.db_object  = db_object
            self.display    = self.VMDisplay(self)
            
            self.id = db_object.id
            
            # State checking stuff
            self._state_check()
            
        def remove(self):
            try:
                self.stop()
            except self.InvalidStateChange:
                pass # Already offline
            
            self.db_session.delete(self.db_object)
            self.db_session.commit()
            
        def start(self):
            if self.state() == 'offline':
                self.__randomize_ports()
                
                arguments = [
                    self.vertibird.qemu, # PROCESS
                    '-monitor',
                    'telnet:{0}:{1},server,nowait'.format(
                        GLOBAL_LOOPBACK,
                        self.db_object.ports['monitor']
                    ),
                    '-nographic',
                    #'-display',
                    #'gtk', # DEBUG ONLY
                    '-serial',
                    'none',
                    '-vnc',
                    '{0}:{1},share=force-shared'.format(
                        GLOBAL_LOOPBACK,
                        self.db_object.ports['vnc'] - QEMU_VNC_ADDS
                    ),
                    '-m',
                    '{0}B'.format(self.db_object.memory),
                    '-boot',
                    'order=cdn,menu=on',
                    '-cpu',
                    shlex.quote(self.db_object.cpu),
                    '-smp',
                    str(self.db_object.cores),
                    '-machine',
                    shlex.quote(self.db_object.machine),
                    '-object',
                    'rng-random,id=rng0,filename=/dev/urandom',
                    '-device',
                    'virtio-rng-pci,rng=rng0',
                    '-vga',
                    shlex.quote(self.db_object.vga),
                    '-device',
                    'lsi53c895a,id=scsi0',
                    '-device',
                    'ahci,id=ahci'
                ]
                
                for cdrom in self.db_object.cdroms:
                    if os.path.isfile(cdrom):
                        arguments += [
                            '-drive',
                            'file={0},media=cdrom'.format(
                                shlex.quote(os.path.abspath(cdrom))
                            )
                        ]
                    else:
                        raise LaunchDependencyMissing(cdrom)
                        
                for key, drive in enumerate(self.db_object.drives):
                    internal_id = self.__random_device_id()
                    
                    if os.path.isfile(drive['path']):
                        if drive['type'] == 'ide':
                            arguments += [
                                '-drive',
                                'id={0},file={1},if=none,format=raw'.format(
                                    internal_id,
                                    shlex.quote(os.path.abspath(drive['path']))
                                ),
                                '-device',
                                'ide-hd,drive={0},bus=ahci.{1}'.format(
                                    internal_id,
                                    key
                                )
                            ]
                        elif drive['type'] == 'scsi':
                            arguments += [
                                '-drive',
                                'id={0},file={1},if=none,format=raw'.format(
                                    internal_id,
                                    shlex.quote(os.path.abspath(drive['path']))
                                ),
                                '-device',
                                'scsi-hd,drive={0},bus=scsi0.{1}'.format(
                                    internal_id,
                                    key
                                )
                            ]
                        elif drive['type'] == 'virtio':
                            arguments += [
                                '-drive',
                                'id={0},file={1},if=virtio,format=raw'.format(
                                    internal_id,
                                    shlex.quote(os.path.abspath(drive['path']))
                                )
                            ]
                    else:
                        raise LaunchDependencyMissing(drive['path'])
                
                # VM LAUNCH
                pid = subprocess.Popen(arguments).pid
                
                self.db_object.pid   = pid
                self.db_object.state = 'online'
                self.db_session.commit()
                
                self.display.connect()
            else:
                raise self.InvalidStateChange('Invalid state for start()!')
        
        def get_properties(self):
            return {
                'memory'    : self.db_object.memory  ,
                'cores'     : self.db_object.cores   ,
                'cpu'       : self.db_object.cpu     ,
                'machine'   : self.db_object.machine ,
                'vga'       : self.db_object.vga     ,
            }
        
        def set_properties(self, properties: dict):
            self.__set_option_offline()
            
            self.db_object.memory    = int(properties['memory' ])
            self.db_object.cores     = int(properties['cores'  ])
            self.db_object.cpu       = str(properties['cpu'    ])
            self.db_object.machine   = str(properties['machine'])
            self.db_object.vga       = str(properties['vga'    ])
            self.db_session.commit()
        
        def attach_cdrom(self, iso: str):
            self.__set_option_offline()
            
            if not os.path.isfile(iso):
                raise LaunchDependencyMissing(iso)
            
            if not (iso in self.db_object.cdroms):
                # Weird appending is required to trigger dirty state
                self.db_object.cdroms = (self.db_object.cdroms + [iso,])
                self.db_session.commit()
                
        def list_cdroms(self):
            return self.db_object.cdroms
            
        def detach_cdrom(self, iso: str):
            self.__set_option_offline()
            
            if (iso in self.db_object.cdroms):
                self.db_object.cdroms = filter(
                    lambda x: x != iso,
                    self.db_object.cdroms
                )
                self.db_session.commit()
                
        def attach_drive(self, img: str, dtype: str = 'ide'):
            self.__set_option_offline()
            
            if not (dtype in ['ide', 'scsi', 'virtio']):
                raise self.InvalidDriveType('No such type {0}.'.format(dtype))
                
            if not os.path.isfile(img):
                raise LaunchDependencyMissing(img)
                
            if not (img in list(map(
                    lambda x: x['path'],
                    self.db_object.drives
                ))):
                    
                self.db_object.drives = self.db_object.drives + [{
                    'path': img,
                    'type': dtype
                },]
                
                self.db_session.commit()
            
        def list_drives(self):
            return self.db_object.drives
            
        def detach_drive(self, img: str):
            self.__set_option_offline()
            
            self.db_object.drives = filter(
                lambda x: x['path'] != img,
                self.db_object.drives
            )
            self.db_session.commit()
            
        def create_or_attach_drive(
                self, img: str, size: int = DEFAULT_DSIZE, dtype: str = 'ide'
            ):
            self.__set_option_offline()
            
            if not os.path.isfile(img):
                self.vertibird.create_drive(img, size)
            else:
                self.attach_drive(img, dtype)
        
        def signal_shutdown(self):
            if self.state() != 'offline':
                self.__send_monitor_command('system_powerdown')
            else:
                raise self.InvalidStateChange('Invalid state for powerdown!')
            
        def signal_reset(self):
            if self.state() != 'offline':
                self.__send_monitor_command('system_reset')
            else:
                raise self.InvalidStateChange('Invalid state for reset!')
            
        def stop(self):
            if self.state() != 'offline':
                try:
                    self.__send_monitor_command('quit')
                except:
                    pass # Could not have initialized yet
                
                try:
                    os.kill(self.db_object.pid, signal.SIGINT)
                except ProcessLookupError:
                    pass # Process does not exist
                
                self.__mark_offline()
            else:
                raise self.InvalidStateChange('Invalid state for stop()!')
            
        def state(self):
            self._state_check()
            
            return self.db_object.state
            
        def _state_check(self):
            # Check if QEMU instance is actually still running
            if self.db_object.state != 'offline':
                try:
                    x = psutil.Process(self.db_object.pid)
                    
                    # Process may not immediately end
                    if x.status() == 'zombie':
                        x.kill()
                        
                        raise psutil.NoSuchProcess(self.db_object.pid)
                except psutil.NoSuchProcess:
                    self.__mark_offline()
            
        def __mark_offline(self):
            self.db_object.pid   = None
            self.db_object.state = 'offline'
            self.db_session.commit()
            
            try:
                self.display.disconnect()
            except AttributeError:
                pass # Display not set up yet
            
        def __send_monitor_command(self, command: str):
            with telnetlib.Telnet(
                GLOBAL_LOOPBACK,
                self.db_object.ports['monitor'],
                TELNET_TIMEOUT_SECS
            ) as session:
                session.read_until(b'(qemu) ')
                session.write(
                    '{0}'.format(command).encode('ascii') + b'\n'
                )
                session.read_until(b'(qemu) ')
                session.close()
            
        def __randomize_ports(self):
            self.db_object.ports = self.vertibird._new_ports()
            self.db_session.commit()
            
        def __random_device_id(self, length: int = 16):
            return ''.join(
                [random.choice(string.ascii_lowercase) for _ in range(length)]
            )
            
        def __set_option_offline(self):
            if self.state() != 'offline':
                raise self.InvalidStateChange('Must be offline to set options')
    
    class VertiVM(Base):
        __tablename__ = 'machines'
        
        id         = Column(String, primary_key=True)
        ports      = Column(PickleType)
        pid        = Column(Integer)
        state      = Column(String, default = 'offline')
        memory     = Column(Integer, default = 134217728)
        cores      = Column(Integer, default = 1)
        cpu        = Column(String, default = 'host')
        machine    = Column(String, default = 'pc')
        vga        = Column(String, default = 'std')
        cdroms     = Column(PickleType, default = [])
        drives     = Column(PickleType, default = [])
    
    def __find_free_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]
    
    def _new_ports(self):
        return {
            'vnc': self.__find_free_port(),
            'monitor': self.__find_free_port()
        }
        
if __name__ == '__main__':
    import cv2
    import numpy as np
    
    x = Vertibird()
    
    y = x.get('019d372a-8331-489e-bc9a-8186e89e1ece')
    
    try:
        y.attach_cdrom(
            '/home/naphtha/Downloads/ubuntu-20.04-desktop-amd64.iso'
        )
        y.create_or_attach_drive('./drives/test.img', 25769803776, 'virtio')
        options = y.get_properties()
        options['memory'] = 2147483648
        options['cores'] = 4
        y.set_properties(options)
        
        y.start()
    except:
        pass
    
    imgGet = (lambda: cv2.cvtColor(np.asarray(
        y.display.capture().convert('RGB')
    ), cv2.COLOR_RGB2BGR))
    
    while y.state() == 'online':
        z = imgGet()
            
        cv2.imshow('image', z)
        cv2.waitKey(34)
        
        #i = (lambda i: 'None' if bool(i) == False else i)(input('>>> '))
        #print(eval(i))
    
    if y.state() == 'online':
        y.stop()
