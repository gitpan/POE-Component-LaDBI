use strict;
use warnings;

use Test;
BEGIN { plan test => 1 };

use IO::File;
use IO::Handle;

use POE;
use POE::Component::LaDBI;

use Data::Dumper;

use vars qw($BASE_CFG_FN $LADBI_ALIAS $TEST_LOG_FN $TEST_TABLE @TABLE_DATA);
require "ladbi_config.pl";

my $CFG = load_cfg_file( find_file_up($BASE_CFG_FN,0) );


my $LOG = IO::File->new($TEST_LOG_FN, "a") or exit 1;
$LOG->autoflush(1);

$LOG->print("### selectall_hash.t\n");

use Data::Dumper;

my $SQL = "SELECT * FROM $TEST_TABLE";
my $KEY = 'phone';

my $OK = 0;

POE::Component::LaDBI->create(Alias => $LADBI_ALIAS)
  or stop_all_tests("Failed: POE::Component::LaDBI->create()\n");

POE::Session->create
  (
   inline_states =>
   {
    _start      => sub {
      my $args = [$CFG->{DSN}, $CFG->{USER}, $CFG->{PASSWD}];
      $LOG->print("_start: >", join(',',@$args), "<\n");
      $_[KERNEL]->post( $LADBI_ALIAS => 'connect',
			SuccessEvent => 'selectall',
			FailureEvent => 'dberror',
			Args => [$CFG->{DSN}, $CFG->{USER}, $CFG->{PASSWD}]
		      );
    },
    _stop       => sub { $LOG->print("_stop: test session died\n"); },
    shutdown    => sub {
      $LOG->print("shutdown\n");
      $_[KERNEL]->post($LADBI_ALIAS => 'shutdown');
    },
    selectall   => sub {
      my ($dbh_id, $datatype, $data) = @_[ARG0..ARG2];
      $_[HEAP]->{dbh_id} = $dbh_id;
      $LOG->print("selectall: dbh_id=$dbh_id\n");
      $_[KERNEL]->post( $LADBI_ALIAS => 'selectall_hash',
			SuccessEvent => 'cmp_results',
			FailureEvent => 'dberror',
			HandleId     => $dbh_id,
			Args         => [ $SQL , $KEY ]
		      );
    },
    cmp_results => sub {
      my ($dbh_id, $datatype, $data) = @_[ARG0..ARG2];
      $LOG->print("cmp_results: dbh_id=$dbh_id\n");
      my $ok = 0;
      my $err = 'success';
      unless ($datatype eq 'NAMED_TABLE') {
	$err = "datatype != 'NAMED_TABLE', datatype=$datatype";
	goto CMP_YIELD;
      }
      unless (defined $data) {
	$err = 'data undefined';
	goto CMP_YIELD;
      }
      $LOG->print(Dumper($data));
      # returns $VAR1 = { 'keyvalN' => { 'colN' => 'colvalN', ... }, ... }
      # where $KEY is one of the column names
      my (@keys) = keys %$data;
      unless (@keys == @TABLE_DATA) {
	$err = 'nrows != '.scalar(@TABLE_DATA).', nrows='.scalar(@keys);
	goto CMP_YIELD;
      }
      my (@data)  = sort { $a->{$KEY} cmp $b->{$KEY} } @{$data}{@keys};
      my (@tdata) = sort { $a->[2] cmp $b->[2] } @TABLE_DATA;
      for (my $i=0; $i<@data; $i++) {
	my $phone  = $data[$i]->{$KEY};
	my $tphone = $TABLE_DATA[$i]->[2];
	unless ($phone eq $tphone) {
	  $err = "not the correct result; expected $tphone; found $phone;";
	  goto CMP_YIELD;
	}
      }
      $ok = 1;
    CMP_YIELD:
      $OK = $ok;
      $LOG->print("cmp_results: $err\n");
      $_[KERNEL]->yield('disconnect');
    },
    disconnect => sub {
      my ($dbh_id) = $_[HEAP]->{dbh_id};
      $LOG->print("disconnect: dbh_id=$dbh_id\n");
      $_[KERNEL]->post( $LADBI_ALIAS => 'disconnect',
			SuccessEvent => 'shutdown'  ,
			FailureEvent => 'dberror'   ,
			HandleId     => $dbh_id
		      );
    },
    dberror    => sub {
      my ($handle_id, $errtype, $errstr, $err) = @_[ARG0..ARG3];
      $OK = 0;
      $LOG->print("dberror: handler id = $handle_id\n");
      $LOG->print("dberror: errtype    = $errtype  \n");
      $LOG->print("dberror: errstr     = $errstr   \n");
      $LOG->print("dberror: err        = $err      \n") if $errtype eq 'ERROR';
      $_[KERNEL]->yield('shutdown');
    },
   }
  )
  or stop_all_tests("Failed to create test POE::Session\n");

$poe_kernel->run();

$LOG->close();

ok($OK);
