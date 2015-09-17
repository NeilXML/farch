#!/usr/bin/perl -w
# File Archiver Script - FARCH
# v0.1  20150403
#
###########################################################################
### includes                                                            ###
###########################################################################
use strict;
use warnings;
use File::Copy qw(copy);
# use Time::Local qw(timelocal);
use Sys::Hostname ;
use Cwd 'abs_path';
use File::Basename;
use Time::Local;
use File::Find;
# use Archive::Zip;
use Archive::Zip qw( :ERROR_CODES );
use Digest::MD5 qw(md5_hex);
use IO::File;
use Config::Properties;
use File::Temp qw/ tempfile tempdir /;

###########################################################################
### Common  Internal Vars                                               ###
###########################################################################
my $vers=1.20;
my $sub="main";

###########################################################################
### Process Arguments and check source csums                            ###
###########################################################################
my $num_args = $#ARGV + 1;
if ($num_args != 2) {
    flogger("Usage: farch.pl [properties_file] [script_config_hash]");
    exit 1 ;
}

my $propFile=$ARGV[0];
my $expectedHash=$ARGV[1];

if ( ! -f $propFile  ) {
        flogger("ERROR", "Properties file $propFile not found");
        exit 3;
}

# Checksum to check for alterations
my $cSum1=md5_hex(do { local $/; IO::File->new("$0")->getline });
my $cSum2=md5_hex(do { local $/; IO::File->new("$propFile")->getline });
my $cSum=md5_hex( $cSum1. $cSum2 );

flogger("DEBUG", "Script csum = ".$cSum1);
flogger("DEBUG", "Props csum = ".$cSum2);
flogger("DEBUG", "Total csum = ".$cSum);

# Checksum to check for alterations
if ( ! ( "$expectedHash" cmp "$cSum" ) == 0  ) {
        flogger("ERROR", "File Archiver $vers checksum failed. Script may have been altered") ;
        flogger("INFO", "Checksum should be: $cSum");
        exit 444;
}
flogger("DEBUG", "Checksum good");

###########################################################################
### Process Properties File and Variables                               ###
###########################################################################
open my $fh, '<', $propFile
    or die "unable to open configuration file";

my $properties = Config::Properties->new();
  $properties->load($fh);

### User parms  inited from props file                                  ###
#
# directory where files reside.
my $filePath ;
#
# file regex to include or not in the zip
# Example my $fileRegex = qr/^amepat-rec-[0-9]{8}-[0-9]{4}/ ;
# my $fileRegex = qr/^amepat-rec-[0-9]{8}-[0-9]{4}/ ;
my $fileRegex ;
#
# how old in days files are before been archived.
my $fileOlderThanDays ;
#
# What is the Archive file name going to start
# with assuming it will end in YYYYMM.zip
my $archivePrefix  ;
#
# Set to 1 if files are not to be deleted or dryRun.
# Files will still be archived and re-archived
my $dryRun = 1 ; # do not actually remove files for testin
#
# How many archive zip files do you want to keep, older ones will be removed
my $removeArchivesOlderThanDays   ;
#
# Leave at least xx of the latest files unarchived and also leave it's month archive there for when it does eventually get archived.
my $minimumUnArchived = 5   ;
#
# Do not actually archive files just delete when meet retentions rule.
my $noArchiveDeleteOnly   ;
### End of User parms                                                   ###

###########################################################################
### Process internal varibales from user variables                      ###
###########################################################################
$filePath  = $properties->getProperty('filePath') || die("Property filePath BAD") ;
$fileRegex = $properties->getProperty('fileRegex') || die("Property fileRegex BAD") ;
$fileOlderThanDays = $properties->getProperty('fileOlderThanDays') || die("Property fileOlderThanDays BAD") ;
$archivePrefix  = $properties->getProperty('archivePrefix') || die("Property archivePrefix BAD") ;
$dryRun = $properties->getProperty('dryRun') || die("Property dryRun BAD") ;
$removeArchivesOlderThanDays = $properties->getProperty('removeArchivesOlderThanDays') || die("Property removeArchivesOlderThanDays BAD") ;
$minimumUnArchived = $properties->getProperty('minimumUnArchived') || die("Property minimumUnArchived BAD") ;
$noArchiveDeleteOnly = trim($properties->getProperty('noArchiveDeleteOnly')) || die("Property noArchiveDeleteOnly BAD") ;

flogger("INFO", "filePath => $filePath");
flogger("INFO", "fileRegex => $fileRegex");
flogger("INFO", "fileOlderThanDays => $fileOlderThanDays");
flogger("INFO", "archivePrefix => $archivePrefix");
flogger("INFO", "dryRun => $dryRun");
flogger("INFO", "removeArchivesOlderThanDays => $removeArchivesOlderThanDays");
flogger("INFO", "minimumUnArchived => $minimumUnArchived");
flogger("INFO", "noArchiveDeleteOnly => $noArchiveDeleteOnly");

# Internal vars relating to files that need to be archived
my $fileOlderThanSecs = $fileOlderThanDays*24*60*60;
my $mTimeToArchive = (getDateSecsNow() -  $fileOlderThanSecs )  ;
my %sortedFileHash ;
my %readyToArchiveFiles ;
my %ameFiles ;
my %zipFiles ;
my $cnt=0 ;
my $logFile='';   # not used at the moment
my $nullFileName = '/dev/null';

# Internal vars relating to arcive/zip files
my $arcFileOlderThanSecs = $removeArchivesOlderThanDays*24*60*60;
my $mTimeToRemoveArchiveFile = (getDateSecsNow() -  $arcFileOlderThanSecs )  ;
my $bn = basename($archivePrefix) ;
my $arcFileRegex = qr/^${bn}[0-9]{6}.zip$/   ;
my %tArcFiles ;   # Populate with all archive files so older ones can be deleted
my %arcFiles ;   # Populate with all archive files so older ones can be deleted
my $arcFileBaseDir = dirname($archivePrefix) ;

###########################################################################
### Work Starts Here                                                    ###
###########################################################################

flogger("INFO", "File Archiver $vers starting");
flogger("INFO", "Archiving $fileRegex files in  $filePath directory, older than $fileOlderThanDays days to \'${archivePrefix}YYMM.zip\'");

# Get a list of the files to be targeted
getFileList( $filePath ) ;

flogger ("DEBUG",$sub.": No of Files in Hash ameFiles : ".keys %ameFiles);
# zip them up if required
%sortedFileHash = sortHash( %ameFiles ) ;
flogger ("DEBUG",$sub.": No of Files in Sorted Hash sortedFileHash : ".keys %sortedFileHash);
%readyToArchiveFiles = hashToBeZipped( %sortedFileHash ) ;
addToZips( %readyToArchiveFiles );

# Remove the source files
# assumes no dies happened in between.
$cnt = removeFiles(%readyToArchiveFiles);

# Finish up with a nice message
if ($cnt > 0 ) {
flogger ("INFO", "$cnt files successfully archived"); }
else { flogger ("INFO", "No files require archiving.") ; }

flogger ("DEBUG",$sub.": arcFileRegex : \'$arcFileRegex'");
flogger ("INFO", "Checking for aged archive files ready for removal");
flogger ("INFO", "Removing $arcFileRegex files, older than $removeArchivesOlderThanDays days");

getArcFileList ( $arcFileBaseDir ) ;
my $cnt2 = removeFiles ( %arcFiles  );
if ($cnt2 > 0 ) {
        flogger ("INFO", "$cnt2 old archive files removed");
}
else { flogger ("INFO", "Zero archive/zip files were removed.") ; }
flogger ("INFO", "File Archiver complete.") ;
exit 0;

###########################################################################
### Finished / End                                                      ###
###########################################################################



###########################################################################
### Subs Start Here                                                     ###
###########################################################################

sub trim {
        my $s = shift; $s =~ s/^\s+|\s+$//g;
        return $s
}
###########################################################################
### Filters and Pre Processors to find files needing to be archived     ###
###########################################################################
sub getFileList {
    flogger ("DEBUG", $sub.": Dir of \'$_[0]\'");
    # Most of the work is done in preprocessor....
    if (-d $_[0] ) {
        find({ getFileFindOptions() } , $_[0] );
   }
   else  { die flogger ("ERROR", "Source directory \'$_[0]\' does not exist");  }
}

sub getFileFindOptions {
  return (
        preprocess => \&preProcFilter,
        wanted => \&fileFilter1,
        no_chdir=>0
    ) ;
}

sub fileFilter1  {
        # If it's not a file ignore again
        # print $File::Find::name;
        # (! -f _ ) && return     ;
        # print $File::Find::name;

        # need a new istat , sorry, too difficult to carry the original across from preprocessor
         my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
                $atime,$mtime,$ctime,$blksize,$blocks)
                = stat($_);
        # Hash key is fullfile/path
        # Hash Key Value1 is : raw mtime (secs since epoch)
        # Hash Key Value2 is : 6 digit file mod time date (YYYYMM) to use as zip archive file name
        # && ($ameFiles{$File::Find::name} = $mtime)
        ( -f _ )
                && ($ameFiles{$File::Find::name}->{2} = getYYMMMStrFromSecs($mtime))
                && ($ameFiles{$File::Find::name}->{1} = $mtime)
                && (return);
         return;
}

sub preProcFilter {
   my @ret;
   foreach (@_)
   {
        if ( -f $_  ) {
                if  ( ( $_ =~ $fileRegex )  ) {
                        flogger ("DEBUG", "File \'$_\' detected by regex");
                        push @ret, $_ ;
                }
        }

    }
    return @ret;
}

########################################################################
### Filters and Pre Processors to find old archives/zips             ###
### needing to be deleted                                            ###
########################################################################
sub getArcFileList {
        my $cnt = 1;
        my $sub = 'getArchiveFileList';
        flogger ("DEBUG", $sub.": Dir of \'$_[0]\'");
        # Most of the work is done in preprocessor....
        if (-d  $_[0]  ) {
                find({ getArcFindOptions() } , $_[0] );
        }
                else  { die flogger ("ERROR", "Source directory for archive files \'$_[0]\' does not exist");
        }
        foreach my $keys (  keys %tArcFiles) {
                $cnt++;
                $arcFiles{$cnt}{"fn"} = $keys  ;
        }

}

sub getArcFindOptions {
  return (
        preprocess => \&arcPreProcFilter,
        wanted => \&arcFileFilter1,
        no_chdir=>0
    ) ;
}

sub arcFileFilter1  {
        # If it's not a file ignore again
        (! -f _ ) && return     ;
        # need a new istat , sorry, too difficult to carry the original across from preprocessor
        my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
                $atime,$mtime,$ctime,$blksize,$blocks)
                = stat($_);

        # Hash key is fullfile/path
        # Hash Key Value is : 6 digit file mod time date (YYYYMM) to use as zip archive file name
        # print "$File::Find::name\n";
        # print "\n $cnt\n";
        $cnt++;
        ( -f _ )
                && ($tArcFiles{$File::Find::name} = $File::Find::name)
                && (return);
         return;

}

sub arcPreProcFilter {
   my @ret;
   foreach (@_)
   {
        if ( -f $_  ) {
              my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
                $atime,$mtime,$ctime,$blksize,$blocks)
                = stat($_);
                # print "$_\n";
                if  ( ( $_ =~ $arcFileRegex ) && ( $mTimeToRemoveArchiveFile > $mtime)  ) {
                       flogger ("DEBUG", "Old archive file \'$_\' ready for removal");
                        push @ret, $_ ;
                }
        }
    }
    return @ret;
}


########################################################################
### sortHash                                                         ###
### sort a hash based on it's value                                  ###
########################################################################

sub sortHash {
        # Creates a hash of zip file unique date numbers,
        # Works on 3 dimensional hash of dfiles requitring archiving. Sorts only value 1 as value 2 is a subset of val 1
        my $sub = "sortHash" ;
        my %retHash ;
        my %lHash = @_ ;
        my $curZip = ""  ; # can be an int
        my $zipCnt = -1;
        my @fileArray ;
        my $counter=0;
        my $argLength = keys %lHash;

        flogger("DEBUG", "$sub: Parsed hash with $argLength keys");
        foreach my $key (sort { $lHash{$b}{1} <=> $lHash{$a}{1} } keys %lHash )  {
                $counter++;
                # print "\n## Val1 ".$lHash{$key}{1}." ## Val2 ".$lHash{$key}{2}."## Key ".$key."\n";
                $retHash{$counter}{"fn"}=$key;
                $retHash{$counter}{"ts"}=$lHash{$key}{1};
                $retHash{$counter}{"zip"}=$lHash{$key}{2};
                flogger ("DEBUG" , "sortHash - Pushing file TS:". $retHash{$counter}{"ts"} . " ZIPKey:". $retHash{$counter}{"zip"} ." Filename:". $retHash{$counter}{"fn"}) ;
        }
        return %retHash ;
}


########################################################################
### addToZips                                                        ###
### adds to zips , including handling temp files                     ###
########################################################################
sub writeToZip {
        if ( "$noArchiveDeleteOnly" eq "true"  ) {
                flogger ("INFO", "writeToZip: Archiving is turned off, remove only. No work for zipping");
                return;
        }
        flogger ("DEBUG", "writeToZip: Args: ".$_[0].", ".$_[1].", ".$_[2]);
        my $zip=$_[0];
        my $tfn=$_[1];
        my $zipFile = $_[2];
        my $status = $zip->writeToFileNamed( $tfn ) ;
        if ( $status != 0 )
        {
                flogger ("ERROR", "Could not write to temp zip file ".$tfn);
                exit 4;
        }

        flogger ("INFO", "Checking temp zip ".$tfn);
        my @goodMembers = checkZip($tfn);
        flogger ("DEBUG" , "addToZips: Copying temp zip file to permanent zip file $tfn ==>> $zipFile") ;
        unless ( copy($tfn, $zipFile ) == 1 ) {
                my $err=$!;
                flogger("ERROR", "Copying $tfn ==>> $zipFile");
                flogger("ERROR", "$err");
                die $err ;
        }
        flogger ("INFO", "Checking new zip ${zipFile}");
        @goodMembers = checkZip($zipFile);
        flogger("INFO", "addToZips: ".$#goodMembers." now in zip file ${zipFile}");
        # all must be good so we can delete temp zip
        flogger ("INFO", "Cleaning up temp zip ".$tfn);
        unless ( (unlink $tfn)  == 1 ) {
                flogger("ERROR" , "Failed to delete temp file ${tfn}");
                exit 88 ;
        }

}

sub addToZips {
        my %lHash = @_ ;
        my $max =  keys %lHash ;
        flogger ("DEBUG", "addToZips: maxKeys $max");
        my $curKey = undef  ;
        my $tfn = undef  ;
        my $tfh = undef ;
        my $zip = undef  ;
        my $zipFile = undef ;
        my $curZipCnt = 0;
        for (my $i=1; $i <= $max ; $i++ ) {
                if ( defined $zip && defined $curKey && ( ! $lHash{$i}{"zip"} eq "$curKey" ) ) {
                        flogger ("DEBUG", "addToZips: Flushing $curZipCnt files to ${zipFile}");
                        writeToZip($zip, $tfn, $zipFile)  ;
                        $curZipCnt=0;
                        $curKey = undef ;
                        $zip = undef ;
                        $tfh = undef;
                        $tfn = undef;
                        $zipFile = undef ;
                }
                if ( ! defined $curKey ) {
                        # new zip to create
                        $curKey = $lHash{$i}{"zip"} ;
                        flogger ("DEBUG" , "Current Key set to $curKey");
                        $zip = Archive::Zip->new();
                        $tfn = "${archivePrefix}".$curKey."_tmp_".getDateSecsNow();
                        $tfh = IO::File->new("> ${tfn}");
                        $zipFile = "${archivePrefix}${curKey}.zip" ;
                        flogger("DEBUG", "Temp zip file is ${tfn}") ;
                        my $chunkSize = Archive::Zip::chunkSize();
                        flogger("DEBUG", "Default Zip chunk size ${chunkSize}");
                        # Archive::Zip::setChunkSize( 4096 );
                        $chunkSize = Archive::Zip::chunkSize();
                        flogger("DEBUG", "Chunk size set to ${chunkSize}");

                        if ( -f $zipFile ) {
                                flogger ("INFO", "Appending to existing zip : ${zipFile}") ;
                                my $status = $zip->read($zipFile);
                                if ( $status != 0 ) {
                                        flogger ("ERROR", "Existing zip cannot be read: ${zipFile}") ;
                                        exit 3;
                                }
                        }
                        else {
                                flogger ("INFO", "Creating new zip : ${zipFile}") ;
                        }
                        flogger ("DEBUG" , "addToZips: New zip environment created for ${curKey}");
                }
                $zip->addFile(  $lHash{$i}{"fn"}  ,  basename( $lHash{$i}{"fn"} ) ,  9 );
                $curZipCnt++;
       }

        # Finally flush rest to zip
        if ( $curZipCnt > 0 ) {
                flogger ("DEBUG", "addToZips: Final flushing $curZipCnt files to ${zipFile}");
                writeToZip($zip, $tfn, $zipFile)  ;
                $curZipCnt=0;
                $curKey = undef ;
                $zip = undef ;
                $tfh = undef;
                $tfn = undef;
                $zipFile = undef ;
        }
}

sub hashToBeZipped {
        my %lHash = @_ ;
        my %rHash ;
        my $retCnt = 0 ;
        # Fristly lets ignore the last xxx files $minimumUnArchived
        my $cnt =  $minimumUnArchived;
        $cnt++ ;
        my $maxKeys =  keys %lHash ;
        flogger ("DEBUG", "hashToBeZipped: maxKeys $maxKeys");
        # The files need to be older than xxx days
        #       fileOlderThanDays
        # Therefore only archive files with a TS of less than now - fileOlderThanDays (all in seconds)
        my $fileOlderThanSecs = $fileOlderThanDays*24*60*60;

        for ( ; $cnt <= $maxKeys ; $cnt++ ) {
                flogger ("DEBUG", "hashToBeZipped: File ".$lHash{$cnt}{"fn"});
                flogger ("DEBUG", "hashToBeZipped: Secs Now ". getDateSecsNow() ) ;
                # print "\nMATH: " . getDateSecsNow() .  " - " . $lHash{$cnt}{"ts"}. " > " . $fileOlderThanSecs . "\n";

                if ( (getDateSecsNow() - $lHash{$cnt}{"ts"} )  >  $fileOlderThanSecs  ) {
                        $retCnt++;
                        $rHash{$retCnt}{"fn"} = $lHash{$cnt}{"fn"}  ;
                        $rHash{$retCnt}{"ts"} = $lHash{$cnt}{"ts"};
                        $rHash{$retCnt}{"zip"} = $lHash{$cnt}{"zip"} ;
                        flogger ("DEBUG", "hashToBeZipped: File No " .$cnt . ": ".$rHash{$retCnt}{"fn"}. " TS " .$rHash{$retCnt}{"ts"}.  " ZIP " .$rHash{$retCnt}{"zip"}. " is ready to be sent to archive");
                }
                else {
                        flogger ("DEBUG", "hashToBeZipped: File ".$lHash{$cnt}{"fn"}." still too young to be archived");
                }
        }
        return %rHash   ;
}

###########################################################################
### checkZip                                                            ###
### check zip is not corrupted                                          ###
###########################################################################
sub checkZip {
        my $zipFile = "@_" ;
        flogger ("INFO", "Checking (reading) zip $zipFile");
        my $zip = Archive::Zip->new();
        $zip->read( "$zipFile" );
        my @members = $zip->members();
        foreach my $mem (@members)
        {
                # print "." ;
                my $fh = IO::File->new();
                $fh->open(">$nullFileName") || die "can't open $nullFileName\: $!\n";
                my $status = $mem->extractToFileHandle($fh);
                if ($status != 0 )
                {
                        flogger ("ERROR", "Zip @_ corrupted, exting immediately");
                        exit 5;
                }
        }
        return @members;
}

sub removeFiles {
        my $cnt = 0 ;
        my %lHash = @_ ;
        flogger ("DEBUG" , "removeFiles: Removing " .  (keys %lHash) . " files");
        foreach my $key ( keys %lHash )  {
                $cnt++;
                if ($dryRun == 0 )
                {
                        flogger ("INFO", "Removing archived file \'". $lHash{$key}{"fn"} ."\'");
                        unlink  $lHash{$key}{"fn"} or die flogger ("ERROR", "Unable to unlink \'" . $lHash{$key}{"fn"} . "\' \($!\)");
                }
                else { flogger ("INFO", "DryRun - Pretending to Delete \'" .  $lHash{$key}{"fn"} . "\'");  }
        }
        # flogger ("INFO", "Successfully removed $cnt files");
        return $cnt;
}


sub getErrors {
        my $sub = 'getErrors';
        my $cmd = '/usr/bin/sh "grep -v "INFO" '.$logFile.'"';
        flogger ("DEBUG", $sub.":Command: ".$cmd);
        my @rc = qx($cmd);
        if ( @rc == 0 ) {
                flogger ("DEBUG", $sub.":No ERRORS or WARNINGS Found");
                return 0;
        }
        flogger ("DEBUG", $sub.": ERRORS or WARNINGS Found, check further");
        # lets check how old the alerts are....
        foreach (@rc) {
                my @thisRc = split(/[\s]+/,"$_") ;
                my $dateStr=$thisRc[0]." ".$thisRc[0];
                flogger ("DEBUG", $sub.": Is this date old ? ".$dateStr);
                if ( isDateStrExpired( $dateStr , 10 ) ) {
                        flogger ("DEBUG", $sub.":DateStr ". $dateStr." IGNORING EXPIRED");
                }
                else {
                        my $shortErrorMsg = $thisRc[2];
                        my $fullMessage='';
                        flogger ("DEBUG", $sub.":DateStr ". $dateStr." MSG ".$shortErrorMsg);
                }
        }
}

###########################################################################
### Date and Time Utilities                                             ###
###                                                                     ###
###########################################################################

sub isDateStrExpired {
        my $sub = 'isDateStrExpired';
        flogger ("DEBUG", $sub.": Checking date ".$_[0]);
        my $dateScalar1 = getDateScalarNow();
        my $dateScalar2 = getDateScalarFromStr($_[0]);
        my $expSecs = $_[1];
        if ( ($dateScalar1 - $dateScalar2) > $expSecs )
        {
                return 1 ;
        }
        return 0;
}

sub getFileModAgeSecs {
        # 1 is true 0 is false;
        my $sub = 'getFileModAgeSecs';
        my $retVal = 0;
        my $file = "@_";
        my $now = getDateSecsNow();
        my $modTime =  getFileModScalar($file) ;
        flogger("DEBUG", $sub.":File Mod time being checked ". $file );
        flogger("DEBUG", $sub.":Now ".$now." => ".getDateStrFromScalar($now,2));
        flogger("DEBUG", $sub.":File Mod Time Scalar:".$modTime." => String:".getDateStrFromScalar($modTime,2)   );
        return ( $now - $modTime )
}

sub getFileModSecs {
        my $sub = 'getFileModStr';
        my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,$atime,$mtime,$ctime,$blksize,$blocks) = stat("@_");
        flogger("DEBUG", $sub.":Last Mod Time on file "."@_"." is ".$mtime);
        return $mtime;
}

sub getLocalTimeFromUtcSecs {
        my $sub = 'getLocalTimeFromUtcSecs';
        flogger("DEBUG", $sub.":"."@_");
        return localtime("@_");
}

sub getDateSecsNow {
        my $sub = 'getDateSecsNow';
        flogger("DEBUG", $sub);
        return time();
}



sub getDateSecsFromStr {
        my $sub='getDateSecFromStr';
        # expecting 2015-04-08 11:55:48.579
        my $argDate = "@_";
        flogger("DEBUG", $sub.":Arg parsed ".$argDate);
        my @strAr = split(/[-:\s.]/, "$argDate") ;
        flogger("DEBUG", $sub.":Found ". @strAr ." elements in array");
        if ( @strAr == 6 ) {
                # add in milliseconds if they don't exist
                 $strAr[6]="000";
        }
        my $year =  $strAr[0];
        my $mon =  $strAr[1];
        my $mday =  $strAr[2];
        my $hour =  $strAr[3];
        my $min =  $strAr[4];
        my $sec =  $strAr[5];
        my $msec =  $strAr[6];

        flogger("DEBUG", $sub." YEAR ".$year) ;
        flogger("DEBUG", $sub." MONTH ".$mon) ;
        flogger("DEBUG", $sub." DAY ".$mday);
        flogger("DEBUG", $sub." HOUR ".$hour) ;
        flogger("DEBUG", $sub." MIN ".$min) ;
        flogger("DEBUG", $sub." SEC ".$sec) ;
        flogger("DEBUG", $sub." MSEC ".$msec ) ;
        my $time = timelocal($sec,$min,$hour,$mday,$mon,$year);
        flogger("DEBUG", $sub.": Return:".$time) ;
        return $time ; ;
}

sub getDateStrFromSecs {
        # argument is a time scalar time from epoch, we dont really care from which epoch.
        my $sub = 'getDateStrFromSecs';
        # flogger("DEBUG", $sub.":Secs: ".$_[0]);
        my ($second, $minute, $hour, $dayOfMonth, $month, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings) = localtime($_[0]);
        my $year = 1900 + $yearOffset;
        $month          = sprintf ("%02d", $month+1) ;
        $hour           = sprintf ("%02d", $hour) ;
        $dayOfMonth     = sprintf ("%02d", $dayOfMonth) ;
        $second         = sprintf ("%02d", $second) ;
        $year           = sprintf ("%04d", $year) ;
        if (@_ > 1)
        {
                if ( $_[1] == 1 )
                {
                        return ($year.$month.$dayOfMonth." ".$hour.":".$minute.".".$second);
                }
                if ( $_[1] == 2 )
                {
                        return ($year."-".$month."-".$dayOfMonth." ".$hour.":".$minute.":".$second);
                }
                if ( $_[1] == 3 )
                {
                        return ($year.$month);
                }
        }
        else
        {
                 return ($year.$month.$dayOfMonth.$hour.$minute) ;
        }
}

sub getYYMMMStrFromSecs {
        my $sub = 'getYYMMStrFromSecs' ;
        # flogger("DEBUG", $sub.":getting special date string for ".$_[0]);
        # flogger("DEBUG", $sub.":returning ".getDateStrFromSecs($_[0],3));
        return getDateStrFromSecs($_[0], 3);
}

sub getDateStrNow {
        #my @months = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
        #my @weekDays = qw(Sun Mon Tue Wed Thu Fri Sat Sun);
        my ($second, $minute, $hour, $dayOfMonth, $month, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings) = localtime();
        my $year = 1900 + $yearOffset;
        $month          = sprintf ("%02d", $month+1) ;
        $hour           = sprintf ("%02d", $hour) ;
        $dayOfMonth     = sprintf ("%02d", $dayOfMonth) ;
        $second         = sprintf ("%02d", $second) ;
        $year           = sprintf ("%04d", $year) ;
        $minute         = sprintf ("%02d", $minute) ;
        # my $theTime = "$hour:$minute:$second, $weekDays[$dayOfWeek] $months[$month] $dayOfMonth, $year";
        if (@_)
        {
                if ( $_[0] == 1 )
                {
                        return ($year.$month.$dayOfMonth." ".$hour.":".$minute.".".$second);
                }
                if ( $_[0] == 2 )
                {
                        return ($year."-".$month."-".$dayOfMonth." ".$hour.":".$minute.":".$second);
                }
        }
        else
        {
                 return ($year.$month.$dayOfMonth.$hour.$minute) ;
        }
}


###########################################################################
### flogger                                                             ###
### Logging utility                                                     ###
###########################################################################

sub flogger {
        # print "No of args passed to flogger is ".@_."\n";
        my $sev = "MSG";
        my $msg ;
        if  ( @_ > 1  )
        {
                ($sev , $msg) = @_;
        }
        else
        {
                 ($msg) = @_;
        }
        if ( ("$sev" cmp "DEBUG") == 0  ) {
                # print getDateStrNow(1)." -".$sev."- ".$msg."\n";
        }
        else {
                print getDateStrNow(1)." -".$sev."- ".$msg."\n";
        }

}
