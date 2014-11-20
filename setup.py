#!/usr/bin/env python

import os
from glob import glob
import platform

if os.environ.get('USE_SETUPTOOLS'):
    from setuptools import setup
    setup_kwargs = dict(zip_safe=0)
else:
    from distutils.core import setup
    setup_kwargs = dict()

data_files=[
    ('share/diamond',                      ['LICENSE', 'README.md'] ),
    ('share/diamond/user_scripts',         [] ),
]

if os.getenv('VIRTUAL_ENV', False):
    data_files.append(('etc/diamond',                        glob('conf/*.conf.*') ))
    data_files.append(('etc/diamond/collectors',             glob('conf/collectors/*') ))
else:
    data_files.append(('/etc/diamond',                       glob('conf/*.conf.*') ))
    data_files.append(('/etc/diamond/collectors',            glob('conf/collectors/*') ))

    if platform.dist()[0] == 'Ubuntu':
        data_files.append(('/etc/init',                      ['debian/upstart/diamond.conf'] ))

def pkgPath(root, path, rpath="/"):
    global data_files
    if not os.path.exists(path):
        return
    files = []
    for spath in os.listdir(path):
        subpath = os.path.join(path, spath)
        spath = os.path.join(rpath, spath)
        if os.path.isfile(subpath):
            files.append(subpath)

    data_files.append((root+rpath, files))
    for spath in os.listdir(path):
        subpath = os.path.join(path, spath)
        spath = os.path.join(rpath, spath)
        if os.path.isdir(subpath):
            pkgPath(root, subpath, spath)

pkgPath('share/diamond/collectors', 'src/collectors')

setup(
    name            = 'diamond',
    version         = '0.2.1',
    url             = 'https://github.com/BrightcoveOS/Diamond',
    author          = 'The Diamond Team',
    author_email    = 'https://github.com/BrightcoveOS/Diamond',
    license         = 'MIT License',
    description     = 'Smart data producer for graphite graphing package',
    package_dir     = {'' : 'src'},
    packages        = ['diamond' , 'diamond.handler'],
    scripts         = glob('bin/*'),
    install_requires= ['configobj'], 
    data_files      = data_files,
    #test_suite      = 'test.main',
    **setup_kwargs
)
