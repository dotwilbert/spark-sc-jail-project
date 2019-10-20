#! /usr/bin/env python

import argparse
import pathlib
import tempfile
import shutil
import subprocess

def check_directory(dn: str) -> bool:
    p = pathlib.Path(dn)
    if not p.exists():
        raise ValueError(f'Directory: {str(p)} does not exist')
    if not p.is_dir():
        raise ValueError(f'Directory: {str(p)} is not a directory')
    return True

def convert_imagefile_to_text(fn: str, wd: str) -> str:
    text_file = f'{wd}/{pathlib.PurePath(fn).stem}'
    r = subprocess.run(['pdftotext', '-layout', '-enc', 'UTF-8', '-nopgbrk', fn, f'{text_file}.txt'], capture_output=True, timeout=5)
    if r.returncode != 0:
        raise IOError(str(r.stderr))
    r.stdout
    return f'{text_file}.txt'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Convert Inmate Population Count Report To Text')
    parser.add_argument('-i', '--infile', help='Input pdf file', required=True)
    parser.add_argument('-o', '--outfile',
                        help='Output text file', required=True)

    cleanup_infile = parser.add_mutually_exclusive_group(required=True)
    cleanup_infile.add_argument(
        '--keep-infile', action='store_true', help="Don't clean up the input file")
    cleanup_infile.add_argument(
        '--delete-infile', action='store_true', help="Clean up the input file")

    args = parser.parse_args()

    if not pathlib.Path(args.infile).exists():
        raise ValueError(f'File: {str(pathlib.PurePath(args.infile))} does not exist')
    
    if not pathlib.Path(args.infile).is_file():
        raise ValueError(f'File: {str(pathlib.PurePath(args.infile))} is not a regular file')

    if not check_directory(pathlib.PurePath(args.outfile).parent):
        exit(1)

    if pathlib.Path(args.outfile).exists():
        print(f'File {args.outfile} exists. Will not overwrite.')
    else:
        workdir = tempfile.mkdtemp()
        text_file = convert_imagefile_to_text(args.infile, workdir)
        shutil.move(pathlib.PurePath(text_file), args.outfile)
        shutil.rmtree(workdir)
        
    if args.delete_infile:
        pathlib.Path(args.infile).unlink()
