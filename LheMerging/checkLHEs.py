'''
Script to check if integrity of LHE files at T2. If corrupted, tries to remove them
'''

import sys
import os
import subprocess
import glob
import random
import re


def getOptions():

  # convention: no capital letters

  from argparse import ArgumentParser

  parser = ArgumentParser(description='', add_help=True)

  parser.add_argument('-v','--ver', type=str, dest='ver', help='version of production, e.g. Bc_LHE_600M_v2', default='Bc_LHE_600M_v2')
  parser.add_argument('--doCleaning', dest='doCleaning', help='also clean the target directory', action='store_true', default=False)
  return parser.parse_args()

def clean(tmpdir):
  try:
    #subprocess.check_output('cd ~/.', shell=True)
    os.chdir('/afs/cern.ch/work/m/mratti/private/LheMerging')
    subprocess.check_output('rm -rf {}'.format(tmpdir), shell=True)
    print 'CLEAN: OK'
  except subprocess.CalledProcessError as e:
    print 'CLEAN: NOT'
    print e
    

if __name__ == "__main__":

  opt = getOptions()


  # check existence of proxy, otherwise results won't be reliable
  proxy = 'ls /tmp/x509up_u60571'
  subprocess.check_output(proxy,shell=True, stderr=subprocess.STDOUT)
  

  xrdprefix = 'root://storage01.lcg.cscs.ch/'
  uberftpprefix = 'gsiftp://storage01.lcg.cscs.ch/'


  #t2path = '/pnfs/lcg.cscs.ch/cms/trivcat/store/user/mratti/Bc_gen/{pl}/'.format(pl=opt.ver)
  t2path = '/pnfs/lcg.cscs.ch/cms/trivcat/store/user/cgalloni/{pl}/'.format(pl=opt.ver)
  #name='bcvegpy_200k_{ijob}.lhe.xz'.format(ijob=ijob)

  ls = 'uberftp -ls {}/{}'.format(uberftpprefix,t2path)
  outls = subprocess.check_output(ls,shell=True)
  #print outls
  matches = re.split('[ \r]',outls)    ### NOTE \r instead of \n !!!!!!
  #print matches
  files = filter(lambda x: 'bcveg' in x, matches)
  #files = filter(lambda x: files.split('/')[-1], files) # pure file name, without the path
  #print files


  copied_files = 0
  good_files = 0
  cleaned_files = 0
  tot_files = len(files) 

  #for log,err,out in zip(logfiles,errfiles,outfiles):
  for i,file in enumerate(files):
 
    print '\n===> WORKING ON FILE: {}'.format(file)
    # only for debugging purposes
    ls = 'uberftp -ls {}/{}/{}'.format(uberftpprefix,t2path,file)
    ls_out = subprocess.check_output(ls, shell=True)
    print ls_out
  
    mark_for_xml_corrupted = False
 
    # copy to tmp
    tmpdir = '/tmp/mratti/{}/'.format(random.randint(1,9999999))
    #print tmpdir
    try:
      subprocess.check_output('mkdir {}'.format(tmpdir), shell=True)
      os.chdir(tmpdir)
      #print '{}/{}/{}'.format(xrdprefix,t2path,file)
      # copy on tmp so that you can do the integrity check at the next step
      copyout = subprocess.check_output('gfal-copy --force {}/{}/{} .'.format(xrdprefix,t2path,file), shell=True)
      if 'FATAL' in copyout  or 'ERROR' in copyout:  
        print 'COPY: NOT', copyout
        continue
      #subprocess.check_output('xrdcp -f {}/{}/{} .'.format(xrdprefix,t2path,file), shell=True)
      #os.system('xrdcp -f {}/{}/{} .'.format(xrdprefix,t2path,file))
      print 'COPY: OK'
      copied_files += 1
    except subprocess.CalledProcessError as e:
      print 'COPY: NOT', e
      clean(tmpdir)
      continue
    # check integrity of file
    try:
      subprocess.check_output('xmllint -noout {}'.format(file), shell=True)
      print 'XMLCHECK: OK'
      good_files += 1
    except subprocess.CalledProcessError as e:
      print 'XMLCHECK: NOT'
      print '  try to remove corrupted file'
      mark_for_xml_corrupted = True

    if mark_for_xml_corrupted:
      rm = 'uberftp -rm {}/{}/{}'.format(uberftpprefix,t2path,file)
      
      if opt.doCleaning:
        try:
          subprocess.check_output(rm, shell=True)
          print 'CLEAN LHE: OK'
          cleaned_files += 1
        except subprocess.CalledProcessError as e:
          print 'CLEAN LHE: NOT'
          print 'plase remove by hand: '
          print rm
          print(e)

    clean(tmpdir)
    
  print '\n\nSummary'
  print 'Total   files :', tot_files 
  print 'Copied  files :', copied_files 
  print 'Good    files :', good_files
  print 'Bad     files :', copied_files-good_files
  print 'Removed files :', cleaned_files

