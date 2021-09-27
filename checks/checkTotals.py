import sys
import os
import subprocess
import glob


if __name__ == "__main__":

  proxy = 'ls /tmp/x509up_u60571'
  subprocess.check_output(proxy,shell=True, stderr=subprocess.STDOUT)

  path = 'gsiftp://storage01.lcg.cscs.ch/pnfs/lcg.cscs.ch/cms/trivcat/store/user/mratti/Bc_gen/'

  productions_to_check = [
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

  path_camilla = 'gsiftp://storage01.lcg.cscs.ch/pnfs/lcg.cscs.ch/cms/trivcat/store/user/cgalloni/'
  camillas_productions_to_check = [
   'Bc_LHE_600M',
   'Bc_LHE_600M_Nov2020',
  ]

  tot_jobs_OK = 0

  for i,prod in enumerate(productions_to_check + camillas_productions_to_check):

    print '\n===> Working on production i={}, label={}'.format(i,prod)

    prod_jobs = 0 

    try:
      path_to_use = path if i<9 else path_camilla
      ls_grep = 'uberftp -ls {} | wc -l'.format(path_to_use+prod)
      ls_grep_out = subprocess.check_output(ls_grep, shell=True, stderr=subprocess.STDOUT)
      prod_jobs = int(ls_grep_out.split('/n')[0])
    except subprocess.CalledProcessError as e:
      print 'error in ls | wc -l, ' , e

    print '      number of files  = {:10}'.format(prod_jobs)
    print '      number of events = {:12} M'.format(prod_jobs*200./1000.)
    tot_jobs_OK += prod_jobs

  #print '\n\n Total '

  print '\n\n Totals:'
  print '      number of files  = {:10}'.format(tot_jobs_OK)
  print '      number of events = {:12} M'.format(tot_jobs_OK*200./1000.)
  print '      if no failure    = {:10} M'.format(34*100)



