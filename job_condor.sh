#/bin/sh 
dir0=$PWD
FirstRun=0;
LastRun=2000;  ## Total jobs for the dataset
writeScript() {

   jobNumber=`echo $1 | awk '{ printf("%d",$1) }'`;
#    jobNumber=`echo $1 | awk '{ printf("%0004d",$1) }'`;
  if [ -d "Bcgen_${jobNumber}" ]; then
    cp template/bcvegpy_set_par.nam Bcgen_${jobNumber}
  else
   echo "inside else"
   cp -r template Bcgen_${jobNumber}

  fi
   cd Bcgen_${jobNumber}

   #cp -f /tmp/x509up_u61204  userproxy
   cp -f /tmp/x509up_u60571  userproxy
   
   ##seed=$RANDOM
   #myhash=$(ps -aux | sha1sum)
   #myint=$((0x${myhash%% *}))
   #seed=${myint##*[+-]}
   seed=$((11001+$jobNumber))
   ##seed=$jobNumber
   sed "s:SEED:$seed:g" -i bcvegpy_set_par.nam

   echo "#!/bin/sh
source /cvmfs/grid.cern.ch/umd-c7ui-latest/etc/profile.d/setup-c7-ui-example.sh
cd /afs/cern.ch/work/m/mratti/private/request/CMSSW_10_2_27/src"> bclhe_${jobNumber}.sh
echo 'eval `scramv1 runtime -sh` ' >> bclhe_${jobNumber}.sh
echo "cd $PWD
export PYTHONHOME=/cvmfs/cms.cern.ch/slc7_amd64_gcc820/external/python/2.7.15/
pwd
../../run
mv bcvegpy.lhe bcvegpy_200k_${jobNumber}.lhe 
xz bcvegpy_200k_${jobNumber}.lhe
globus-url-copy -v -cd bcvegpy_200k_${jobNumber}.lhe.xz  gsiftp://storage01.lcg.cscs.ch///pnfs/lcg.cscs.ch/cms/trivcat/store/user/mratti/Bc_gen/Bc_LHE_400M_v5/" >> bclhe_${jobNumber}.sh
echo 'if [ $? -ne 0 ] ; then' >> bclhe_${jobNumber}.sh
echo "      ls $PWD
      globus-url-copy -v -cd bcvegpy_200k_${jobNumber}.lhe  gsiftp://storage01.lcg.cscs.ch///pnfs/lcg.cscs.ch/cms/trivcat/store/user/mratti/Bc_gen/Bc_LHE_400M_v5/
      rm bcvegpy_200k_${jobNumber}.lhe
      rm bcvegpy*ev*     
else   
      rm bcvegpy_200k_${jobNumber}.lhe.xz
      rm bcvegpy*ev* 
fi " >> bclhe_${jobNumber}.sh
chmod u+xrw bclhe_${jobNumber}.sh
# qsub -q cmssq bclhe_${jobNumber}.sh
####################################  if you work in lxplusxxx.cern.ch
####################################  qsub -q cmssq  bclhe_${jobNumber}.sh should be repalced with bsub -q 1nw bclhe_${jobNumber}.sh 
cd ..
}
for (( i=FirstRun; i<=LastRun; i++ )) {
   writeScript $i;
}
