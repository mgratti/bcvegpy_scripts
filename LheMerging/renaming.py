import subprocess
import glob

nfiles = {}


tags = [
"BcToNMuX_NToEMuPi_SoftQCD_b_mN3p0_ctau1000p0mm_10M",
"BcToNMuX_NToEMuPi_SoftQCD_b_mN3p0_ctau100p0mm_10M" ,
"BcToNMuX_NToEMuPi_SoftQCD_b_mN3p0_ctau10p0mm_10M" ,
"BcToNMuX_NToEMuPi_SoftQCD_b_mN3p0_ctau1p0mm_10M"  ,
"BcToNMuX_NToEMuPi_SoftQCD_b_mN4p5_ctau100p0mm_10M" ,
"BcToNMuX_NToEMuPi_SoftQCD_b_mN4p5_ctau10p0mm_10M" ,
"BcToNMuX_NToEMuPi_SoftQCD_b_mN4p5_ctau1p0mm_10M"  ,
"BcToNMuX_NToEMuPi_SoftQCD_b_mN4p5_ctau0p1mm_10M"  ,
"BcToNMuX_NToEMuPi_SoftQCD_b_mN5p5_ctau10p0mm_10M" ,
"BcToNMuX_NToEMuPi_SoftQCD_b_mN5p5_ctau1p0mm_10M"  ,
"BcToNMuX_NToEMuPi_SoftQCD_b_mN5p5_ctau0p1mm_10M"  ,
"BcToNMuX_NToEMuPi_SoftQCD_b_mN5p5_ctau0p01mm_10M" ,
]

ver = 'VTEST'
path = ver + '/results*lhe.gz'
all_files = glob.glob(path)


files_ns   = [3,1,1,1,3,1,2,3,74,80,104,101]

mypos = 0
batches = []
for i in files_ns:
  batches.append( all_files[mypos:mypos+i] )
  mypos = mypos+i

for ib,batch in enumerate(batches):
  for i,ifile in enumerate(batch):
    target_file = ver + '/' + tags[ib] + '_'  + str(i) + '.lhe.gz'
    cmd = 'mv {} {}'.format(ifile,target_file)
    #print cmd
    subprocess.check_output(cmd,shell=True)
