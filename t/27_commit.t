use strict;
use warnings;

use Test;
BEGIN { plan test => 1 };

use IO::File;
use IO::Handle;

use POE;
use POE::Component::LaDBI;

use Data::Dumper;

use vars qw($BASE_CFG_FN $LADBI_ALIAS $TEST_LOG_FN $TEST_TABLE @EXTRA_ROW);
require "ladbi_config.pl";

my $CFG = load_cfg_file( find_file_up($BASE_CFG_FN,0) );


my $LOG = IO::File->new($TEST_LOG_FN, "a") or exit 1;
$LOG->autoflush(1);

$LOG->print("### commit.t\n");

use Data::Dumper;

my $DO_SQL = <<"EOSQL";
DELETE FROM $TEST_TABLE WHERE firstname = ? AND lastname = ?
EOSQL

my $DO_ATTR = { AutoCommit => 0 };
my (@DO_VALUES) = @EXTRA_ROW[0,1];

my $SELECT_SQL = "SELECT * FROM $TEST_TABLE WHERE firstname = ? AND lastname = ?";

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
			SuccessEvent => 'do_sql',
			FailureEvent => 'dberror',
			Args => [$CFG->{DSN}, $CFG->{USER}, $CFG->{PASSWD},
				 { AutoCommit => 0 } ]
		      );
    },
    _stop       => sub { $LOG->print("_stop: test session died\n"); },
    shutdown    => sub {
      $LOG->print("shutdown\n");
      $_[KERNEL]->post($LADBI_ALIAS => 'shutdown');
    },
    do_sql   => sub {
      my ($dbh_id, $datatype, $data) = @_[ARG0..ARG2];
      $_[HEAP]->{dbh_id} = $dbh_id;
      $LOG->print("do_sql: dbh_id=$dbh_id\n");
      $_[KERNEL]->post( $LADBI_ALIAS => 'do',
			SuccessEvent => 'first_cmp_results',
			FailureEvent => 'dberror',
			HandleId     => $dbh_id,
			Args         => [ $DO_SQL, $DO_ATTR, @DO_VALUES ]
		      );
    },
    first_cmp_results => sub {
      my ($dbh_id, $datatype, $data) = @_[ARG0..ARG2];
      $LOG->print("first_cmp_results: dbh_id=$dbh_id\n");
      my $err = 'success';
      unless ($datatype eq 'RV') {
	$err = "datatype != 'RV', datatype=$datatype";
	goto CMP_YIELD_ONE;
      }
      unless (defined $data) {
	$err = 'data undefined';
	goto CMP_YIELD_ONE;
      }
      $LOG->print(Dumper($data));
      unless ($data == 1) {
	$err = "data != 1, data == $data";
	goto CMP_YIELD_ONE;
      }
    CMP_YIELD_ONE:
      $LOG->print("first_cmp_results: $err\n");
      $_[KERNEL]->yield('commit');
    },
    commit => sub {
      my ($dbh_id) = $_[HEAP]->{dbh_id};
      $LOG->print("commit: dbh_id=$dbh_id\n");
      $_[KERNEL]->post( $LADBI_ALIAS => 'commit',
			SuccessEvent => 'confirm_commit',
			FailureEvent => 'dberror'   ,
			HandleId     => $dbh_id
		      );
    },
    confirm_commit => sub {
      my ($dbh_id, $datatype, $data) = @_[ARG0..ARG2];
      $LOG->print("confirm_commit: dbh_id=$dbh_id\n");
      $_[KERNEL]->post( $LADBI_ALIAS => 'selectall',
			SuccessEvent => 'second_cmp_results',
			FailureEvent => 'dberror'   ,
			HandleId     => $dbh_id,
			Args => [ $SELECT_SQL, $DO_ATTR, @DO_VALUES ]
		      );
    },
    second_cmp_results => sub {
      my ($dbh_id, $datatype, $data) = @_[ARG0..ARG2];
      $LOG->print("second_cmp_results: dbh_id=$dbh_id\n");
      my $err = 'success';
      my $ok = 0;
      unless ($datatype eq 'TABLE') {
	$err = "datatype != 'TABLE', datatype=$datatype";
	goto CMP_YIELD_TWO;
      }
      unless (defined $data) {
	$err = 'data undefined';
	goto CMP_YIELD_TWO;
      }
      $LOG->print(Dumper($data));
      unless (@$data == 0) {
	$err = "nrows != 0; nrows == ".scalar(@$data);
	goto CMP_YIELD_TWO;
      }
      $ok = 1;
    CMP_YIELD_TWO:
      $OK = $ok;
      $LOG->print("second_cmp_results: $err\n");
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
