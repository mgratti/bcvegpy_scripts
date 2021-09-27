import sys
import os
import subprocess
import glob
import random


def getOptions():

  # convention: no capital letters

  from argparse import ArgumentParser

  parser = ArgumentParser(description='', add_help=True)

  parser.add_argument('-v','--ver', type=str, dest='ver', help='version of production, e.g. Bc_LHE_600M_v2', default='Bc_LHE_600M_v2')
  parser.add_argument('--doCleaning', dest='doCleaning', help='also clean the target directory', action='store_true', default=False)
  return parser.parse_args()


def checkSizeLhe(size_lhe):
  exp_size_lhe = 156200240 
  tol =           10000000
  return ( size_lhe < exp_size_lhe + tol and size_lhe > exp_size_lhe - tol)

def checkFile(filepath):
  '''
  checks existence and size of remote file, with uberftp protocol
  '''
  isThere = False
  foundsize = 0

  ls = 'uberftp -ls ' + filepath
  try:
    ls_out = subprocess.check_output(ls, shell=True, stderr=subprocess.STDOUT)
    isThere = True
  except subprocess.CalledProcessError as e:
    #print(e) 
    pass

  size = 'uberftp -size ' + filepath
  try:
    size_out = subprocess.check_output(size, shell=True, stderr=subprocess.STDOUT) 
    foundsize = float(size_out.split('\n')[0])
  except subprocess.CalledProcessError as e:
    #print(e)
    pass

  return isThere,foundsize

if __name__ == "__main__":

  opt = getOptions()

  # script to check log files, 
  # and decide on the fate of the potentially existing file at T2 

  # check existence of proxy, otherwise results won't be reliable
  proxy = 'ls /tmp/x509up_u60571'
  subprocess.check_output(proxy,shell=True, stderr=subprocess.STDOUT)
  

  # 
  logpath = '/afs/cern.ch/work/m/mratti/private/Bc_gen/bcvegpy_scripts/bcvegpy2.2b/test/outCondor/' + opt.ver + '/'
  if not os.path.isdir(logpath): raise RuntimeError('input path does not exist')

  logfiles = glob.glob(logpath+'condor_job_*.log')
  errfiles = glob.glob(logpath+'condor_job_*.error')
  outfiles = glob.glob(logpath+'condor_job_*.out')
  if len(logfiles)==0 or len(errfiles)==0 or len(outfiles)==0:
    raise RuntimeError('Either error files or log files do not exist')
  tot = len(errfiles)
  count = 0

  #for log,err,out in zip(logfiles,errfiles,outfiles):
  for log in logfiles:
    count +=1

    mark_for_log_aborted = False
    mark_for_out_present = False
    mark_for_err_present = False
    mark_for_program_finished = False
    mark_for_absence_xz = False
    mark_for_recovery_lhe = False
    mark_for_successful_recovery_lhe = False
    mark_for_deletion_lhe = False
    mark_for_deletion_xz = False
    mark_for_abortion = False
    #size,size_lhe=0,0
    
    # check same i
    ijob = log.split('_KINDEX')[0].split('_')[-1]
    #if (ijob != err.split('_KINDEX')[0].split('_')[-1]) or (ijob != out.split('_KINDEX')[0].split('_')[-1]):
    #  raise RuntimeError('bad logic, please check')

    print '\n==> Working on job i: {}   progress: {}/{}'.format(ijob,count,tot)    

    # verify from the .log if the job was not aborted
    with open(log) as logf:
      logl = logf.readlines()
      if len(filter(lambda x: 'Job was aborted.' in x, logl)) == 1:
        mark_for_log_aborted = True

    # verify presence of err and log files
    outfname = logpath+'condor_job_{}_KINDEX.out'.format(ijob)
    errfname = logpath+'condor_job_{}_KINDEX.error'.format(ijob)
    
    if os.path.isfile(outfname):
      mark_for_out_present = True
      with open(outfname) as outf:
        outl = outf.readlines()
        if len(filter(lambda x: 'program finished' in x, outl)) == 1:
          mark_for_program_finished = True
          ##continue 
          ## if program has not finished correctly it's useless to go further
    if os.path.isfile(errfname): 
      mark_for_err_present = True
      with open(errfname) as errf:
        errl = errf.readlines()
        if len(filter(lambda x: 'Fortran runtime error' in x, errl)) == 1:
          mark_for_abortion = True
   
    # check existence of output file (xz) and size
    t2path = 'gsiftp://storage01.lcg.cscs.ch/pnfs/lcg.cscs.ch/cms/trivcat/store/user/mratti/Bc_gen/'
    name='{pl}/bcvegpy_200k_{ijob}.lhe.xz'.format(pl=opt.ver,ijob=ijob)
    isThere,size = checkFile(t2path+name)
    if not isThere: 
      mark_for_absence_xz = True

    if size > 7.5E06:
      pass # all OK
    else:
      mark_for_deletion_xz=True
    
    # check if it is possible to recover the lhe 
    isThere_lhe,size_lhe = checkFile(t2path+name[:-3])
    if isThere_lhe:
      if mark_for_program_finished and mark_for_absence_xz:
        if checkSizeLhe(size_lhe):
          mark_for_recovery_lhe = True
      else:
        mark_for_deletion_lhe = True

    #try to recover the lhe
    if mark_for_program_finished and mark_for_absence_xz and mark_for_recovery_lhe:
      
      try:
        tmpdir = '/tmp/mratti/{}/'.format(random.randint(1,10000))
        os.system('mkdir ' + tmpdir)
        destlhe = tmpdir + 'test.lhe'
        copy = 'globus-url-copy -v -cd ' + t2path + name.split('.xz')[0] + ' ' + destlhe
        copy_out = subprocess.check_output(copy, shell=True)
        compress = 'xz '+ destlhe 
        compress_out = subprocess.check_output(compress, shell=True)
        copyback = 'globus-url-copy -v -cd ' + destlhe + '.xz  ' + t2path + name
        copyback_out = subprocess.check_output(copy, shell=True)
        mark_for_successful_recovery_lhe = True
      except subprocess.CalledProcessError as e:
        mark_for_deletion_lhe = True # just to make sure there anything bad is deleted
        print '    something went wrong during lhe recovery'

   
    
    # Print summary info:
    print '   -1/5 log not aborted       = {}'.format(not mark_for_log_aborted)
    #print '   -2/5 out present           = {}'.format(mark_for_out_present)
    #print '   -1/5 err present           = {}'.format(mark_for_err_present)
    print '    0/5 program not aborted   = {}'.format(not mark_for_abortion)
    print '    1/5 program finished      = {}'.format(mark_for_program_finished)
    print '    2/5 output file present   = {}'.format(not mark_for_absence_xz)
    print '    3/5 output big enough     = {} {}'.format(not mark_for_deletion_xz, size/1.0E06)
    print '    4/5 try to recover LHE    = {} {}'.format(mark_for_recovery_lhe, size_lhe/1.0E06)
    print '    5/5 recovery successful   = {}'.format(mark_for_successful_recovery_lhe)
    print '        deletion of lhe       = {}'.format(mark_for_deletion_lhe) 
    print '        deletion of lhe.xz    = {}'.format(mark_for_deletion_xz) 
    # Cleaning up
    if (mark_for_deletion_xz or mark_for_deletion_lhe) or mark_for_abortion or mark_for_log_aborted or not mark_for_program_finished:
      print '\n    Clean up: going to remove incomplete or bad files'

      if mark_for_deletion_xz or mark_for_abortion or mark_for_log_aborted or not mark_for_program_finished: # add the case 
        isThere,size=checkFile(t2path+name)
        if isThere:
          rm = '    uberftp -rm ' + t2path + name
          print rm
          if opt.doCleaning:  
            try:
              rm_out = subprocess.check_output(rm, shell=True)
            except subprocess.CalledProcessError as e:
              print 'removal did not work'

      if mark_for_deletion_lhe or mark_for_abortion or mark_for_log_aborted or not mark_for_program_finished:
        isThere_lhe,size_lhe=checkFile(t2path+name[:-3])
        if isThere_lhe:
          rm = '    uberftp -rm ' + t2path + name[:-3]
          print rm
          if opt.doCleaning:  
            try:
              rm_out = subprocess.check_output(rm, shell=True)
            except subprocess.CalledProcessError as e:
              print 'removal did not work'

    

