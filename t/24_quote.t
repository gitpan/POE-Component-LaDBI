use strict;
use warnings;

use Test;
BEGIN { plan test => 1 };

use IO::File;
use IO::Handle;

use POE;
use POE::Component::LaDBI;

use Data::Dumper;

use vars qw($NO_DB_TESTS_FN $BASE_CFG_FN $LADBI_ALIAS $TEST_LOG_FN $TEST_TABLE @TABLE_DATA);
require "ladbi_config.pl";

if (find_file_up($NO_DB_TESTS_FN, 1)) {
  skip("skip no database tests", 1);
  exit 0;
}

my $CFG = load_cfg_file( find_file_up($BASE_CFG_FN,0) );


my $LOG = IO::File->new($TEST_LOG_FN, "a") or exit 1;
$LOG->autoflush(1);

$LOG->print("### quote.t\n");

use Data::Dumper;

my $QUOTE_STRING = 'jim';
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
			SuccessEvent => 'quote',
			FailureEvent => 'dberror',
			Args => [$CFG->{DSN}, $CFG->{USER}, $CFG->{PASSWD}]
		      );
    },
    _stop       => sub { $LOG->print("_stop: test session died\n"); },
    shutdown    => sub {
      $LOG->print("shutdown\n");
      $_[KERNEL]->post($LADBI_ALIAS => 'shutdown');
    },
    quote   => sub {
      my ($dbh_id, $datatype, $data) = @_[ARG0..ARG2];
      $_[HEAP]->{dbh_id} = $dbh_id;
      $LOG->print("quote: dbh_id=$dbh_id\n");
      $_[KERNEL]->post( $LADBI_ALIAS => 'quote',
			SuccessEvent => 'cmp_results',
			FailureEvent => 'dberror',
			HandleId     => $dbh_id,
			Args => [ $QUOTE_STRING ]
		      );
    },
    cmp_results => sub {
      my ($dbh_id, $datatype, $data) = @_[ARG0..ARG2];
      $LOG->print("cmp_results: dbh_id=$dbh_id\n");
      my $ok = 0;
      my $err = 'success';
      unless ($datatype eq 'SQL') {
	$err = "datatype != 'SQL', datatype=$datatype";
	goto CMP_YIELD;
      }
      unless (defined $data) {
	$err = 'data undefined';
	goto CMP_YIELD;
      }
      $LOG->print(Dumper($data));
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
