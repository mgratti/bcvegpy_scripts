#/bin/csh 
dir0=$PWD
FirstRun=0;
LastRun=14;  ## Total jobs for the dataset
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
   
   sed "s:RAND:$RANDOM:g" -i bcvegpy_set_par.nam

   echo "#!/bin/zsh
cd $PWD
${dir0}/../run" > bclhe_${jobNumber}.sh
chmod u+xrw bclhe_${jobNumber}.sh
# qsub -q cmssq bclhe_${jobNumber}.sh
####################################  if you work in lxplusxxx.cern.ch
####################################  qsub -q cmssq  bclhe_${jobNumber}.sh should be repalced with bsub -q 1nw bclhe_${jobNumber}.sh 
cd ..
}
for (( i=FirstRun; i<=LastRun; i++ )) {
   writeScript $i;
}
