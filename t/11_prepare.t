use strict;
use warnings;

use Test;
BEGIN { plan test => 1 };

use IO::File;
use IO::Handle;

use POE;
use POE::Component::LaDBI;

use vars qw($NO_DB_TESTS_FN $BASE_CFG_FN $LADBI_ALIAS $TEST_LOG_FN $TEST_TABLE);
require "ladbi_config.pl";

if (find_file_up($NO_DB_TESTS_FN, 1)) {
  skip("skip no database tests", 1);
  exit 0;
}

my $CFG = load_cfg_file( find_file_up($BASE_CFG_FN,0) );


my $LOG = IO::File->new($TEST_LOG_FN, "a") or exit 1;
$LOG->autoflush(1);

$LOG->print("### prepare.t\n");

use Data::Dumper;

my $SQL = "SELECT phone FROM $TEST_TABLE WHERE firstname = ?";

my $OK = 0;

POE::Component::LaDBI->create(Alias => $LADBI_ALIAS)
  or stop_all_tests("Failed: POE::Component::LaDBI->create()\n");

POE::Session->create
  (
   inline_states =>
   {
    _start     => sub {
      my $args = [$CFG->{DSN}, $CFG->{USER}, $CFG->{PASSWD}];
      $LOG->print("_start: >", join(',',@$args), "<\n");
      $_[KERNEL]->post( $LADBI_ALIAS => 'connect',
			SuccessEvent => 'prepare',
			FailureEvent => 'dberror',
			Args => [$CFG->{DSN}, $CFG->{USER}, $CFG->{PASSWD}]
		      );
    },
    _stop     => sub { $LOG->print("_stop: test session died\n"); },
    shutdown   => sub {
      $LOG->print("shutdown\n");
      $_[KERNEL]->post($LADBI_ALIAS => 'shutdown');
    },
    prepare    => sub {
      my ($dbh_id, $datatype, $data) = @_[ARG0..ARG2];
      $_[HEAP]->{dbh_id} = $dbh_id;
      $LOG->print("prepare: $SQL\n");
      $_[KERNEL]->post( $LADBI_ALIAS => 'prepare',
			SuccessEvent => 'disconnect',
			FailureEvent => 'dberror',
			HandleId     => $dbh_id,
			Args => [ $SQL ]
		      );
    },
    disconnect => sub {
      my ($sth_id, $datatype, $data) = @_[ARG0..ARG2];
      my $dbh_id = $_[HEAP]->{dbh_id};
      $LOG->print("disconnected: dbh_id=$dbh_id;\n");
      $_[KERNEL]->post( $LADBI_ALIAS => 'disconnect',
			SuccessEvent => 'success'   ,
			FailureEvent => 'dberror'   ,
			HandleId     => $dbh_id
		      );
    },
    success    => sub {
      $LOG->print("success\n");
      $OK = 1;
      $_[KERNEL]->yield('shutdown');
    },
    dberror    => sub {
      my ($handle_id, $errtype, $errstr, $err) = @_[ARG0..ARG3];
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
