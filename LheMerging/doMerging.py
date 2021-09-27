'''
Script to do LHE merging of Bc files for the BHNL analysis
to be run from lxplus with valid grid certificate
You need writing rights on the given eos directory
'''

import sys
import os
import subprocess
import glob
import re
import itertools

class Batch(object):
  '''
  Batch of jobs
  '''
  def __init__(self,files,name,nfxjob,outdir,proddir,ver):

    self.name = name
    self.files = files
    self.nfxjob = nfxjob
    self.njobs = int(len(self.files)/self.nfxjob)
    self.job_files =  [ files[i:i+self.nfxjob] for i in range(0,len(files), self.nfxjob)]
    #print 'DEBUG', range(0,len(files), self.nfxjob)
    #second_to_last = range(0,len(files), self.nfxjob)[-2]
    #self.job_files[-1] = files[second_to_last:-1]
    self.outdir  = outdir  + '/' 
    self.proddir = proddir + '/' + self.name + '/'
    self.ver = ver

  def stamp(self):
    print '======> BATCH {}'.format(self.name)
    print '  Tot # files     = {}'.format(len(self.files))
    print '  First file      = {}'.format(self.files[0])
    print '  Last file       = {}'.format(self.files[-1])
    print '  # of jobs       = {}'.format(self.njobs)
    print '  # of files job  = {}'.format(self.nfxjob)
    print '  -1 file of -1 j = {}'.format(self.job_files[-1][-1])


  def prepareDirs(self):
    # directories where to run the jobs 
    if not os.path.isdir(self.outdir):
      os.system('mkdir -p ' + self.outdir) 
  
    # directory where to store the submitter, the templates and the logs
    if not os.path.isdir(self.proddir):
      os.system('mkdir -p ' + self.proddir + '/logs') 

  def writeScripts(self):

    for ijob in range(0,self.njobs):
      script_name = 'merge_{ij}.sh'.format(ij=ijob)

      script_files = [ '"{}"'.format(ifile) for ifile in self.job_files[ijob] ]

      script_lines_p1 = [
  
        '#!/bin/bash',
        'export XRDCSCS=root://storage01.lcg.cscs.ch/',
        'export XRDEOS=root://eoscms.cern.ch/',
        'export PL="{ver}"',
        'export MYSCRATCH=/tmp/mratti/merge_$PL_$3_$2/',
        'export MYPRODDIR=/afs/cern.ch/work/m/mratti/private/LheMerging/$PL/$3/',
        'export X509_USER_PROXY=$1',
        '',
        '# copy files from cs cs to the worker node',
        'declare -a files=(',
      ]
  
      script_lines_p2 = [
        ')',
        '',
        'clean () {{',
        '  rm -rf $MYSCRATCH',
        '}}',
        '',
        'if [[ ! -d $MYSCRATCH ]]; ',
        'then ',
        '  mkdir -p $MYSCRATCH ',
        'else',
        '  rm -rf $MYSCRATCH ',
        '  mkdir -p $MYSCRATCH ',
        'fi',
        '',
        '',
        'cp $MYPRODDIR/../../SIMPLI_merge.pl $MYSCRATCH',
        'if [ $? -eq 0 ]; then echo "Copy merger: OK"; else echo "Copy merger: NOT"; clean; exit $?; fi',
        'cd $MYSCRATCH',
        '',
        '',
        'for ((i = 0; i < ${{#files[@]}}; ++i)); ',
        'do',
        '  echo "======> FILE $i"',
        '  # copy',
        '  xrdcp $XRDCSCS/${{files[$i]}} ./file_$i.lhe.xz',
        '  if [ $? -eq 0 ]; then echo "Copy: OK"; else echo "Copy: NOT"; clean; exit $?; fi',
        '  # unzip it',
        '  xz -d -v ./file_$i.lhe.xz',
        '  if [ $? -eq 0 ]; then echo "Unzip: OK"; else echo "Unzip: NOT"; clean; exit $?; fi',
        '  # rezip it',
        '  gzip ./file_$i.lhe',
        '  if [ $? -eq 0 ]; then echo "Gzip: OK"; else echo "Gzip: NOT"; clean; exit $?; fi',
        'done',
        '',
        '# variable with all files',
        'gzfiles=""',
        'for ((i = 0; i < ${{#files[@]}}; ++i));',
        'do',
        '  gzfiles+=" ./file_$i.lhe.gz"',
        'done',
        'echo $gzfiles',
        '',
        '',
        '# merge them all ',
        'echo "======> MERGING"',
        './SIMPLI_merge.pl $gzfiles results_$3_$2.lhe.gz banner_$2.txt',
        'if [ $? -eq 0 ]; then echo "Merge: OK"; else echo "Merge: NOT"; clean; exit $?; fi',
        '',
        '# Copy the merged file to EOS',
        'xrdcp -f results_$3_$2.lhe.gz $XRDEOS/eos/cms/store/group/phys_exotica/BHNLs/lheMerging/$PL/results_$3_$2.lhe.gz',
        'if [ $? -eq 0 ]; then echo "Copy results: OK"; else echo "Copy results: NOT"; clean; exit $?; fi',
        '',
        '# Copy banner back',
        'cp banner_$2.txt $MYPRODDIR/logs',
        'if [ $? -eq 0 ]; then echo "Copy banner: OK"; else echo "Copy banner: NOT"; clean; exit $?; fi',
        '',
        '# Cleaning',
        'clean',
        'if [ $? -eq 0 ]; then echo "Cleaning: OK"; else echo "Cleaning: NOT";  exit $?; fi',
      ]

      script_lines = script_lines_p1 + script_files + script_lines_p2
      script = '\n'.join(script_lines)
      script = script.format(ver=self.ver)
      with open(self.proddir + script_name, 'w') as fscr:
       fscr.write(script)
  
    print('===> Created scripts for batch submission')

  def writeSubmitter(self):
    submitter_lines = [
      'universe              = vanilla',
      'executable            = merge_$(ProcId).sh',
      'output                = ./logs/merge_$(ProcId).out',
      'error                 = ./logs/merge_$(ProcId).out',
      'log                   = ./logs/merge_$(ProcId).log',
      '',
      'proxy_path            = /afs/cern.ch/work/m/mratti/private/LheMerging/x509up   ',
      'arguments             = $(proxy_path) $(ProcId) {name}',
      'should_transfer_files = YES',
      'transfer_input_files  = $(proxy_path)',
      '',
      '+JobFlavour           = "workday" ',
      '',
      'queue                 {njobs}',
    ]

    submitter = '\n'.join(submitter_lines)
    submitter = submitter.format(name=self.name,njobs=self.njobs)

    with open(self.proddir + 'submit.sub', 'w') as fsub:
      fsub.write(submitter)

    print('===> Created submitter\n')

  def doSubmit(self):
    os.chdir(self.proddir)
    condor_out = subprocess.check_output('condor_submit submit.sub', shell=True)
    batchid = condor_out.split('cluster ')[1].split('.')[0]
    print('===> Submitted {}'.format(batchid))
    return batchid
    #4 job(s) submitted to cluster 408342.

def getOptions():

  from argparse import ArgumentParser

  parser = ArgumentParser(description='', add_help=True)
  parser.add_argument('-v','--ver', type=str, dest='ver', help='version of production, e.g. V00', default='V00')
  parser.add_argument('--nbatches', type=int, dest='nbatches', help='number of batches to divide the jobs in', default=8)
  parser.add_argument('--nfxjob', type=int, dest='nfxjob', help='number of files for each job', default=5)
  parser.add_argument('--dosubmit', dest='dosubmit', help='submit to slurm', action='store_true', default=False)
  return parser.parse_args()


### globals
ftp_prefix = 'gsiftp://storage01.lcg.cscs.ch/'
xrd_prefix = 'root://storage01.lcg.cscs.ch/'


if __name__ == "__main__":

  opt = getOptions()

  # check existence of proxy, otherwise results won't be reliable
  proxy = 'ls /tmp/x509up_u60571'
  subprocess.check_output(proxy,shell=True, stderr=subprocess.STDOUT)
  subprocess.check_output('cp /tmp/x509up_u60571 x509up', shell=True, stderr=subprocess.STDOUT)

  outpath = '/eos/cms/store/group/phys_exotica/BHNLs/lheMerging/'
  outdir = outpath + opt.ver + '/'
  proddir = './' + opt.ver + '/'

  # paths where the .lhe files are stored
  inpaths = {}
  inpaths[1] = '/pnfs/lcg.cscs.ch/cms/trivcat/store/user/mratti/Bc_gen/'
  inpaths[2] = '/pnfs/lcg.cscs.ch/cms/trivcat/store/user/cgalloni/'

  productions = {}
  productions[1] = [
   #'Bc_LHE_firstRun',
   #'Bc_LHE_secondRun',
   'Bc_LHE_600M',
   'Bc_LHE_600M_v2',
   'Bc_LHE_200M',
   'Bc_LHE_400M',
   'Bc_LHE_400M_v2',
   'Bc_LHE_400M_v3',
   'Bc_LHE_400M_v4',
   'Bc_LHE_400M_v5',
   'Bc_LHE_400M_v6',
  ]
  productions[2] = [
   'Bc_LHE_600M',
   'Bc_LHE_600M_Nov2020',
  ]

  all_files = []
  for key,prods in productions.items():
    for prod in prods:
      path = ftp_prefix + inpaths[key] + prod + '/*lhe.xz'
      print '========> Working on path', path
      ls = 'uberftp -ls {}'.format(path)
      outls = subprocess.check_output(ls,shell=True)
      matches = re.split('[ \n]',outls)        
      files = filter(lambda x: 'pnfs' in x, matches)
      all_files.append(files)

  # merge # apparently a well performing one
  flat_files = list(itertools.chain.from_iterable(all_files))

  print '===> Info on files: nfiles={}'.format(len(flat_files))
  if len(flat_files) < 18000: print 'WARNING TOO FEW FILES'

  nfxbatch = int(len(flat_files)/opt.nbatches)
  batches_files = [ flat_files[i:i+nfxbatch] for i in range(0,len(flat_files),nfxbatch)]
  #batches_files[-1] = flat_files[:-1]
  # this way some files could end up in no batch, but ok...

  ids = []

  for ib,batch_files in enumerate(batches_files):
    name = 'b{}'.format(ib)
    mybatch = Batch(name=name,files=batches_files[ib],nfxjob=opt.nfxjob,outdir=outdir,proddir=proddir,ver=opt.ver) 
    mybatch.stamp()
    mybatch.prepareDirs()
    mybatch.writeScripts()
    mybatch.writeSubmitter()
    #if opt.dosubmit:
    #ids.append( name + ' ' + mybatch.doSubmit())
  
  #if opt.dosubmit:  
  #  with open('jobs.txt', 'w') as fid:
  #    fid.write('\n'.join(ids)) 
      




