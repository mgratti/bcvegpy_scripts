Useful scripts to produce LHE files for Bc meson, using BCVEGPY version 2.2b (generator can be dowloaded [here](https://cernbox.cern.ch/index.php/s/0igtc8X3mf2mR0x)).  

Instructions below are a development of what presented [here](https://indico.cern.ch/event/238056/contributions/1552957/attachments/400031/556227/bcvegpy.pdf), and allow to use the package with Condor batch system on lxplus. Please check out those instructions first and get familiar with Bcvegpy.

Parameters for the Bc generation are set in the file ``bcvegpy2.2b/bcvegpy_set_par.nam``.
An example of cfg is as in bcvegpy_set_par_example.nam.

To produce multiple LHE files and submit on condor queues on lxplus, edit `job_condor.sh` and `condor_multiple.cfg` to your needs, then: 
``` 
cd test
sh job_condor.sh
condor_submit condor_multiple.cfg
```

Please note:
  * first, do an interactive run with `igentype=1`, `NUMOFEVENTS=10`. Copy the resulting `data/*` into `template/data/.`, so that it can be used for the other runs
  * second, run all batch jobs by setting `igentype=2`, `NUMOFEVENTS=wanted_events`.


After the productions are done, you can run several scripts to check the sanity of the production, see [./checks](./checks).
However the most important check to be run was actually found out later: i.e. the integrity of the files. This is done in [./LheMerging](./LheMerging):
- first run [checkLHEs.py](./LheMerging/checkLHEs.py) to check the integrity of the files and remove the corrupted ones
- then run [doMerging.py](./LheMerging/doMerging.py) to merge them and copy them to /eos/
- you might also need to run the renaming in the end, when the files are copied on /eos/, see [renaming.py](./LheMerging/renaming.py)
