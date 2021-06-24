#!/usr/bin/perl -w
#use strict;
use DBI;

$ip=$ARGV[0] || '--help';
$dbname=$ARGV[1] || 'start';
$dbuser=$ARGV[2] || 'pgsql';

#Default to Unknown Status
my $status=3;

my $vacuum_count=0;
my $alter_count=0;
my $update_count=0;
my $insert_count=0;
my $select_count=0;
my $longidle_count=0;
my $idle_count=0;
my $nonidle_count;
my $drop_count=0;
my $create_count=0;
my $truncate_count=0;
my $unknown_count=0;
my $resting_count=0;
my $copy_count=0;

my $count=0;
my $total_count=0;
my $detail=0;
my $short_query;

my $msg='';
my $msg_query_details='';
my $msg_counts='';

my ($datname,$query,$duration,$slow,$username);
#192.168.3.9 template1 iclive 

if ($ip !~ /\d+/)
{
	print "Provide a IP or hostname of a server to check - got args: 0:$ARGV[0], 1:$ARGV[1], 2:$ARGV[2]\n";
	exit 3;
}
else
{
	#print "Server is $server\n";
	#Connect to Database
	my $Con = "DBI:Pg:dbname=$dbname;host=$ip";
	my $Dbh = DBI->connect($Con, $dbuser, '', {RaiseError =>1}) || die "Unable to access Database $dbname on host $ip as user $dbuser.\nError returned was: ". $DBI::errstr;

	my $sql="SELECT datname,current_query,timeofday()::TIMESTAMP-query_start, (CASE WHEN timeofday()::TIMESTAMP-query_start > INTERVAL '5 minutes' THEN TRUE ELSE FALSE END) AS slow,usename FROM pg_stat_activity;";
# WHERE current_query NOT ILIKE 'SELECT%' AND current_query NOT ILIKE '%<IDLE>%';";
	my $sth = $Dbh->prepare($sql);
	$sth->execute();
	while (($datname,$query,$duration,$slow,$username) = $sth->fetchrow()) {
		if ($slow =~ /1/i)
		{
			if ($query =~/\<IDLE\>/i)
			{
				$longidle_count++;
			}
			else
			{
				$detail=1;
			}
		}

		#Try to categorize queries
		if ($query =~/^SELECT/i)
		{
			$select_count++;
		}
		elsif ( ($query =~/\<IDLE\>/i) || (length($query) == 0) )
		{
			$idle_count++;
		}
		elsif ($query =~/^INSERT/i)
		{
			$insert_count++;
		}
		elsif ($query =~/^UPDATE/i)
		{
			$update_count++;
			$detail=1;
		}
		elsif ($query =~/^VACUUM/i)
		{
			$vacuum_count++;
			$detail=1;
		}
		elsif ($query =~/^CREATE/i)
                {
			$create_count++;
			$detail=1;
		}
		elsif ($query =~/^DROP/i)
                {
			$drop_count++;
			$detail=1;
		}
		elsif ($query =~/^TRUNCATE/i)
                {
			$truncate_count++;
			$detail=1;
		}
		elsif ($query =~/^ALTER/i)
		{
			$alter_count++;
			$detail=1;
		}
		elsif ($query =~/^COPY/i)
		{
			$copy_count++;
			$detail=1;
		}
		# I was attempting to catch any queries with no status, apparently it isn't working
		# The $query field looks like a bunch of spaces
		elsif ($query =~/[\t\s]/)
		{
			$resting_count++;
			$detail=1;
		}
		else
		{
			$unknown_count++;
			$detail=1;
		}

		# if detail is set we do stuff
		if ($detail==1)
		{
			$detail=0;
			$count++;
			$short_query=substr($query,0,18);
			$msg_query_details .= " ,$username doing $short_query on $datname for $duration";
		}
		$total_count++;
	}
	$Dbh->disconnect;

	$nonidle_count=$total_count-$idle_count;
	
	if ($count > 3)
	{
		$status=2;
	}
	elsif ($count > 1)
	{
		$status=1;
	}
	else
	{
		$status=0;
	}
	
	if ($longidle_count > 0)
	{
		$msg_counts="$longidle_count long IDLEs $msg_counts";
	}
	if ($resting_count > 0)
	{
		$msg_counts="$resting_count resting $msg_counts";
	}

	if ($select_count > 0)
	{
		$msg_counts="$select_count SELECTs $msg_counts";
	}

	if ($insert_count > 0)
	{
		$msg_counts="$insert_count INSERTs $msg_counts";
	}

	if ($update_count > 0)
	{
		$msg_counts="$update_count UPDATEs $msg_counts";
	}

	if ($vacuum_count > 0)
	{
		$msg_counts="$vacuum_count VACUUMs $msg_counts";
	}

	if ($alter_count > 0)
	{
		$msg_counts="$alter_count ALTERs $msg_counts";
	}

	if ($drop_count > 0)
	{
		$msg_counts="$drop_count DROPs $msg_counts";
	}

	if ($create_count > 0)
	{
		$msg_counts="$create_count CREATEs $msg_counts";
	}

	if ($truncate_count > 0)
	{
		$msg_counts="$truncate_count TRUNCATEs $msg_counts";
	}
	if ($copy_count > 0)
	{
		$msg_counts="$copy_count COPYs $msg_counts";
	}

	if (length($msg_counts) > 1)
	{
		$msg_counts="($msg_counts)";
	}

	$msg="$nonidle_count of $total_count connections are active $msg_counts $msg_query_details";
# 1 WARNING
# 2 CRITICAL
# 3 UNKNOWN

	print $msg;
	exit $status;
}
