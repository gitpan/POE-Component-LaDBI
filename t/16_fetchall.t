use strict;
use warnings;

use Test;
BEGIN { plan test => 1 };

use IO::File;
use IO::Handle;

use POE;
use POE::Component::LaDBI;

use vars qw($BASE_CFG_FN $LADBI_ALIAS $TEST_LOG_FN $TEST_TABLE @TABLE_DATA);
require "ladbi_config.pl";

my $CFG = load_cfg_file( find_file_up($BASE_CFG_FN,0) );


my $LOG = IO::File->new($TEST_LOG_FN, "a") or exit 1;
$LOG->autoflush(1);

$LOG->print("### fetchall.t\n");

use Data::Dumper;

my $SQL = "SELECT phone FROM $TEST_TABLE WHERE firstname = ?";
my $FNAME = 'jim';
my $PHONE = '111-555-1111';

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
			SuccessEvent => 'prepare',
			FailureEvent => 'dberror',
			Args => [$CFG->{DSN}, $CFG->{USER}, $CFG->{PASSWD}]
		      );
    },
    _stop       => sub { $LOG->print("_stop: test session died\n"); },
    shutdown    => sub {
      $LOG->print("shutdown\n");
      $_[KERNEL]->post($LADBI_ALIAS => 'shutdown');
    },
    prepare     => sub {
      my ($dbh_id, $datatype, $data) = @_[ARG0..ARG2];
      $LOG->print("prepare: $SQL\n");
      $_[HEAP]->{dbh_id} = $dbh_id;
      $_[KERNEL]->post( $LADBI_ALIAS => 'prepare',
			SuccessEvent => 'execute',
			FailureEvent => 'dberror',
			HandleId     => $dbh_id,
			Args => [ $SQL ]
		      );
    },
    execute    => sub {
      my ($sth_id, $datatype, $data) = @_[ARG0..ARG2];
      $LOG->print("execute: sth_id=$sth_id\n");
      $_[KERNEL]->post( $LADBI_ALIAS => 'execute',
			SuccessEvent => 'fetchall',
			FailureEvent => 'dberror',
			HandleId     => $sth_id,
			Args => [ $FNAME ]
		     );
    },
    fetchall    => sub {
      my ($sth_id, $datatype, $data) = @_[ARG0..ARG2];
      $LOG->print("fetch: sth_id=$sth_id\n");
      $_[KERNEL]->post( $LADBI_ALIAS => 'fetchall',
			SuccessEvent => 'cmp_results',
			FailureEvent => 'dberror',
			HandleId     => $sth_id
		      );
    },
    cmp_results => sub {
      my ($sth_id, $datatype, $data) = @_[ARG0..ARG2];
      my $ok = 0;
      my $err = 'success';
      unless ($datatype = 'TABLE') {
	$err = "datatype != 'TABLE', datatype=$datatype";
	goto CMP_YIELD;
      }
      unless (defined $data) {
	$err = "data undefined";
	goto CMP_YIELD;
      }
      unless (@$data == 1) {
	$err = "nrows != 1, nrows=".scalar(@$data);
	goto CMP_YIELD;
      }
      unless (@{$data->[0]} == 1) {
	$err = "nelts != 1, nelts=".scalar(@{$data->[0]});
	goto CMP_YIELD;
      }
      unless ($PHONE eq $data->[0]->[0]) {
	$err = "not the correct result; expected $PHONE; found ".$data->[0]->[0].";";
	goto CMP_YIELD;
      }
      $ok = 1;
    CMP_YIELD:
      $OK = $ok;
      $LOG->print("cmp_results: $err\n");
      $_[KERNEL]->yield('finish', $sth_id);
    },
    finish     => sub {
      $_[KERNEL]->post( $LADBI_ALIAS => 'finish',
			SuccessEvent => 'disconnect',
			FailureEvent => 'dberror',
			HandleId => $_[ARG0]
		      );
    },
    disconnect => sub {
      my ($sth_id, $datatype, $data) = @_[ARG0..ARG2];
      my $dbh_id = $_[HEAP]->{dbh_id};
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
