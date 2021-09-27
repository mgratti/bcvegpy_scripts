#!/usr/bin/perl -w

################################################################################
# SIMPLI_merge.pl
#  Maria Giulia Ratti
# 
# Based on merge.pl by Richard Corke 
# DESCRIPTION : script to merge to LHE events files, as obtained from BCVEGPY, xsec information is not merged
# USAGE :
# ./merge.pl eventfile1.lhe.gz eventfile2.lhe.gz ... result.lhe.gz banner.txt
# Note that result can be eventfile1, eventfile2, ...
# OUTPUT : merged file, banner
################################################################################

use Compress::Zlib;
use List::Util qw[min max];


###########################################################################
# Initialisation
###########################################################################
# Set tag names for later
#my $begin_header = '<header>';
#my $end_header   = '</header>';
#my $begin_event  = '<event>';
#my $end_event    = '</event>';
#my $begin_init   = '<init>';
my $end_init     = '</init>';
my $end_all      = '</LesHouchesEvents>';
#my $event_norm   = 'EMPTY'; 


# Parse the command line arguments
if ( $#ARGV < 2 ) {
  die "This script must be called with at least three filenames as arguments!\n";
}
my $bannerfile = pop(@ARGV); # remove last element from array and pass it on
my $outfile = pop(@ARGV);    # as a result now @ARGV only contains the files to merge


###########################################################################
# The first file is written out with no changes to the header, init block
# All events from other files are then written out as they are
###########################################################################

$gzout    = gzopen($outfile, "w") || die ("Couldn't open file $outfile\n");
open(BANNER, ">$bannerfile")      || die ("Couldn't open file $outfile\n");

$stage = 0;
$filecount = 0;

foreach $infile (@ARGV) {
  $gzin = gzopen($infile, "r") || die ("Couldn't open file $infile\n");
  print BANNER "\n =====> Working on file $infile";
  print        "\n =====> Working on file $infile";

  while (1) {
    $gzbytes = $gzin->gzreadline($gzline);
    if ($gzbytes == -1) { die("Error reading from file $infile\n"); }
    if ($gzbytes == 0)  { last; }

    # Pre-events (copy only if it's the first file)
    if ($stage == 0){ 
      if ($gzline =~ m/$end_init/) { $stage = 2; }
    }
    # Event blocks (copy)
    if ($stage == 2){
      if ($gzline =~ m/$end_all/) { $stage = 1; } 
    }
    # Information not to be copied
    if ($stage == 1){
      #print ("\nStage=$stage\n");
      if ($gzline =~ m/$end_init/) { $stage = 2; }
      next;
    }
    
    # Write out the line
    $gzout->gzwrite($gzline);
  }

  $filecount++;
}

# Write out closing tag and close
$gzout->gzwrite("</LesHouchesEvents>\n");
$gzout->gzclose();

print BANNER "\n\nNumber of merged files=$filecount\n";
print        "\n\nNumber of merged files=$filecount\n";

exit(0);

