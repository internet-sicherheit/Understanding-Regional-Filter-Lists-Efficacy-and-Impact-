from glob import glob

files = glob('*.pdf')

for file in files:
   proc = Popen('./embeded.sh %s' % file, shell=True)
