Useful scripts to produce LHE files for Bc meson, using BCVEGPY version 2.2b (generator can be dowloaded [here](https://cernbox.cern.ch/index.php/s/0igtc8X3mf2mR0x)).  

Instructions below are a development of what presented [here](https://indico.cern.ch/event/238056/contributions/1552957/attachments/400031/556227/bcvegpy.pdf), and allow to use the package with Condor batch system on lxplus.

Parameters for the Bc generation are set in the file ``bcvegpy2.2b/bcvegpy_set_par.nam``.
An example of cfg is as in bcvegpy_set_par_example.nam.

To produce multiple LHE files and submit on condor queues on lxplus:
``` 
cd test
sh job_condor.sh
condor_submit condor_multiple.cfg
```

The number of jobs is set into job_condor.sh.  
The number of events per job is set into bcvegpy_set_par.nam
