import os, sys, re, argparse
import itertools
import shutil
import tempfile, tarfile
import h5py

class fast5_Index():
    def __init__(self, index_file=None, tmp_prefix=None):
        self.index_file = index_file
        self.tmp_prefix = tmp_prefix
        if index_file and not os.path.exists(index_file):
            raise RuntimeError("[Error] Raw fast5 index file {} not found.".format(index_file))
        elif index_file:
            with open(index_file, 'r') as fp:
                self.index_dict = {id:path for path,id in [line.split('\t') for line in fp.read().split('\n') if line]}
        else:
            self.index_dict = None
            
            
            
    def index(input, recursive=False, output_prefix="", tmp_prefix=None):
        
        input_files = []
        # scan input
        if os.path.isfile(input):
            input_files.append(input)
        else:
            if recursive:
                input_files.extend([os.path.join(dirpath, f) for dirpath, _, files in os.walk(input) for f in files if f.endswith('.fast5')])
            else:
                input_files.extend(glob.glob(os.path.join(input, '*.fast5')))
    
                
         # index all provided files
        for input_file in input_files:
            input_relative = os.path.normpath(os.path.join(output_prefix,
                             os.path.dirname(os.path.relpath(input_file, start=input)),
                             os.path.basename(input_file)))
            
            
        reads = self.get_ID_multi(input_file)
        for f, (group, ID) in zip([input_relative] * len(reads), reads):
            yield '\t'.join((os.path.join(f,group), ID))
        
                
    def get_ID_multi(f5_file):
        with h5py.File(f5_file, 'r') as f5:
            reads = []
            for group in f5:
                s = f5[group + "/Raw/"].visit(lambda name: name if 'Signal' in name else None)
                ID = (str(f5[group + "/Raw/" + s.rpartition('/')[0]].attrs['read_id'], 'utf-8'))
                reads.append((group, ID))
            return reads

class main():
    def __init__(self):
        parser = argparse.ArgumentParser(
        description='Nanopore raw fast5 index',
        usage='''storage_fast5Index.py <command> [<args>]
Available commands are:
   index        Index batch(es) of bulk-fast5
''')
        parser.add_argument('command', help='Subcommand to run')
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command', file=sys.stderr)
            parser.print_help(file=sys.stderr)
            exit(1)
        getattr(self, args.command)(sys.argv[2:])

    def index(self, argv):
        parser = argparse.ArgumentParser(description="Fast5 Index")
        parser.add_argument("input", help="Input batch or directory of batches")
        parser.add_argument("--recursive", action='store_true', help="Recursively scan input")
        parser.add_argument("--out_prefix", default="", help="Prefix for file paths in output")
        parser.add_argument("--tmp_prefix", default=None, help="Prefix for temporary data")
        args = parser.parse_args(argv)

        for record in fast5_Index.index(args.input, recursive=args.recursive, output_prefix=args.out_prefix, tmp_prefix=args.tmp_prefix):
            print(record)





if __name__ == "__main__":
    main()