
use Test;
BEGIN { plan test => 1 };

use vars qw($BASE_CFG_FN @CFG_PARAMS);
require "ladbi_config.pl";

my $cfg_fn = find_file_up($BASE_CFG_FN,1);

unless (defined $cfg_fn) {
  print "Bail out!\n", "Couln't find the config file, $BASE_CFG_FN\n";
  exit 0;
}

my $cfg = load_cfg_file($cfg_fn);

unless (defined $cfg) {
  print "Bail out!\n", "#Failed to load config file, $cfg_fn\n";
  exit 0;
}

for my $k (@CFG_PARAMS) {
  unless (defined $cfg->{$k}) {
    print "Bail out!\n", "#Key, $k, not defined\n";
    exit 0;
  }
}

ok(1);
