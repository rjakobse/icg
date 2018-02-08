#!perl

use strict;
use warnings;

use DBI;
use Data::Dumper;

use IO::File;

sybaseData();
insert_into_oracle();

my %data  = ();

sub insert_into_oracle
{
   my $dbi = DBI->connect("dbi:Oracle:DATAHUB",'mssqljump','mssqljump',{PrintError => 0});
   die "Unable for connect to server $DBI::errstr" unless $dbi;
   
   my $sql = qq{DELETE FROM bfly_beloeb};
   my $sth = $dbi->prepare($sql);
      $sth->execute();
   foreach my $loebenr (keys %data)
   {
      my $tot = $data{$loebenr};
         $sql = qq{INSERT INTO bfly_beloeb VALUES ( $loebenr,$tot )};
         $sth = $dbi->prepare($sql);
         $sth->execute();
   }
}
sub sybaseData
{
   my $lbn = get_data_by_nr(0);
   print $lbn,"\n";
   print scalar keys %data;
}
sub get_data_by_nr
{
   my ($lnr) = @_;

   my $dbi = DBI->connect("dbi:ODBC:BUTTER",'readonly','readonly',{PrintError => 0});
   die "Unable for connect to server $DBI::errstr" unless $dbi;
   $lnr ||= 0;

   my $sql = qq{
                   SELECT loebenr, tot_beloeb_float
                   FROM   vicenhedsokument
                   WHERE  loebenr > $lnr
                   ORDER BY loebenr
               };
   my $sth = $dbi->prepare($sql);
      $sth->execute();
   while (my $ray_ref = $sth->fetchrow_arrayref())
   {
      $ray_ref->[0] ||= 0;
      $ray_ref->[1] ||= 0;
      $lnr = $ray_ref->[0] if $ray_ref->[0] > $lnr;
      $data{$ray_ref->[0]} = $ray_ref->[1];
   }
   return $lnr;
}

1;


