use strict;
use warnings;
use Data::Dumper;
use List::Util 'shuffle';
use Text::CSV;
use Digest::MD5 qw(md5_hex);

my $serum="serum_olink_clean_2024-10-23.csv";
my $demog="demographics_clean_2024-10-23.csv";
my %GROUP=(
    "ASYMPTOMATIC AT RISK"          => "Group_1",
    "AMYOTROPHIC LATERAL SCLEROSIS" => "Group_2",
    "DISEASE CONTROL"               => "Group_3",
    "PRIMARY LATERAL SCLEROSIS"     => "Group_4",
    "HEALTHY CONTROL"               => "Group_5",
    "FRONTOTEMPORAL DEMENTIA"       => "Group_6"
    );
my %A_OR_B=(
    "DEAD"     => "A",
    "CENSORED" => "B"
);
my @anonymization_col_names = (
    "IMCM_ID",
    "GROUP",
    "AGE_AT_SAMPLING",
    "AGE_AT_DEAD_OR_CENSORED",
    "DEAD_OR_CENSORED",
    "SERUM_OLINK_MANIFEST"
);
my %anonymization = map { $_ => 1 } @anonymization_col_names;
srand(42);
my $random_integer = int(rand(10)) + 1;
my %dem_olink_id_map;
my %singleton_ids_map;
my %singleton_vals_map;
my @singleton_triplet;
my %dem_map;
my @remaining_col_data;
my $csv_dem = Text::CSV->new({
    binary     => 1,
    auto_diag  => 1,
    sep_char   => ',',
    eol        => "\n",
    quote_char => '"',
    always_quote => 1,
}) or die "Cannot use CSV: " . Text::CSV->error_diag();
open(my $DEM_TEST,">","demographics_test.csv") or die "can't write to demography file: $!";
open(my $DEM,"<","$demog") or die "can't open the demography csv file";
my $dem_header_ref = $csv_dem->getline($DEM);
$csv_dem->column_names(@$dem_header_ref);
my @remaining_cols = grep { defined($_) && $_ ne '' && !exists $anonymization{$_} } @$dem_header_ref;
my @dem_file_cols = ("","IMCM_ID","GROUP","AGE_AT_SAMPLING","AGE_AT_A_OR_B","A_OR_B","SERUM_OLINK_MANIFEST", @remaining_cols);
$csv_dem->print($DEM_TEST, \@dem_file_cols) or die "Failed to write row: " . $csv_dem->error_diag();
my $row_number = 1;
while (my $row_hr = $csv_dem->getline_hr($DEM)) {
    my $imcm_id = $row_hr->{IMCM_ID};
    my $grp = $row_hr->{GROUP};
    my $age_at_sam = $row_hr->{AGE_AT_SAMPLING};
    my $age_at_doc = $row_hr->{AGE_AT_DEAD_OR_CENSORED};
    my $doc = $row_hr->{DEAD_OR_CENSORED};
    my $ser_ol_man = $row_hr->{SERUM_OLINK_MANIFEST};
    my @olink_man_ID_tmp = split(/\_/,$ser_ol_man);
    my $new_ser_ol_man;
    if (@olink_man_ID_tmp == 3){
      my $first = md5_hex($olink_man_ID_tmp[0]);
      my $second = md5_hex($olink_man_ID_tmp[1]);
      $new_ser_ol_man = $imcm_id."_".$first."_".$second."_".$olink_man_ID_tmp[-1];
    }
    else {
      $new_ser_ol_man = "NA";
    }
    my $new_age_at_sam = $age_at_sam + $random_integer;
    my $new_age_at_doc = $age_at_doc + $random_integer;
    $dem_olink_id_map{$ser_ol_man}=$new_ser_ol_man;
    my @fields = ($row_number,$imcm_id,$GROUP{$grp},$new_age_at_sam,$new_age_at_doc,$A_OR_B{$doc},$new_ser_ol_man);
    $dem_map{$row_number} = [@fields];
    my @remaining_col_values;
    foreach my $col (@$dem_header_ref) {
        next if $anonymization{$col};
        push @remaining_col_values, $row_hr->{$col};
    }
    push @remaining_col_data, [@remaining_col_values];
    if (exists $singleton_ids_map{$imcm_id}){
        $singleton_ids_map{$imcm_id}++;
    } else {
        $singleton_ids_map{$imcm_id}=1;
    }
    $row_number++;
}
close $DEM;
foreach my $row_num(sort {$a<=>$b} keys %dem_map){
    if ($singleton_ids_map{$dem_map{$row_num}[1]} == 1){
        push @singleton_triplet, [$dem_map{$row_num}[3],$dem_map{$row_num}[4],$dem_map{$row_num}[5]];
    }
}
my @shuffled_singleton_triplet = shuffle(@singleton_triplet);
my @shuffled_remaining_col_data = shuffle(@remaining_col_data);
foreach my $row_num(sort {$a<=>$b} keys %dem_map){
    my @random_remining_val = @{shift @shuffled_remaining_col_data};
    if ($singleton_ids_map{$dem_map{$row_num}[1]} == 1){
        my $singleton_imcm_id = $dem_map{$row_num}[1];
        my $singleton_grp_id = $dem_map{$row_num}[2];
        my $singleton_ser_man_id = $dem_map{$row_num}[-1];
        my @random_val = @{shift @shuffled_singleton_triplet};
        #my @fields = ($row_num,$singleton_imcm_id,$singleton_grp_id,$random_val[0],$random_val[1],$random_val[2],$singleton_ser_man_id,@random_remining_val);
        my @fields = ($row_num,$singleton_imcm_id,$singleton_grp_id,$dem_map{$row_num}[3],$dem_map{$row_num}[4],$dem_map{$row_num}[5],$singleton_ser_man_id);
        #my @fields = ($row_num,$singleton_imcm_id,$singleton_grp_id,$dem_map{$row_num}[3],$random_val[1],$random_val[2],$singleton_ser_man_id);
        $dem_map{$row_num} = [@fields];
    }
    if ($singleton_ids_map{$dem_map{$row_num}[1]} != 1){
        #my @fields = ($row_num,$dem_map{$row_num}[1],$dem_map{$row_num}[2],$dem_map{$row_num}[3],$dem_map{$row_num}[4],$dem_map{$row_num}[5],$dem_map{$row_num}[6],@random_remining_val);
        my @fields = ($row_num,$dem_map{$row_num}[1],$dem_map{$row_num}[2],$dem_map{$row_num}[3],$dem_map{$row_num}[4],$dem_map{$row_num}[5],$dem_map{$row_num}[6]);
        $dem_map{$row_num} = [@fields];
    }
}
foreach my $row_num(sort {$a<=>$b} keys %dem_map){
  $csv_dem->print($DEM_TEST, \@{$dem_map{$row_num}}) or die "Failed to write row: " . $csv_dem->error_diag();
}
################################################################################
my $csv_ser = Text::CSV->new({
    binary     => 1,
    auto_diag  => 1,
    sep_char   => ',',
    eol        => "\n",
    quote_char => '"',
    always_quote => 1,
}) or die "Cannot use CSV: " . Text::CSV->error_diag();
open(my $SER_TEST,">","serum_test.csv") or die "can't write to serum file: $!";
open(my $SER,"<","$serum") or die "can't open the serum csv file";
my $header_ref = $csv_ser->getline($SER);
my @rec_header = @$header_ref;
my $header_num = scalar(@rec_header);
my @new_serum_headers=($rec_header[0],$rec_header[1]);
for my $i (1 .. $header_num-2) {
    my $column_name = protein_col_name($i);
    push @new_serum_headers, $column_name;
}
$csv_ser->print($SER_TEST, \@new_serum_headers) or die "Failed to write row: " . $csv_ser->error_diag();
while (my $row = $csv_ser->getline($SER)) {
    my ($samID, $plate, $protein1, @cols) = @$row;
    my @shuffled_vals = shuffle(@cols);
    my @fields = ($dem_olink_id_map{$samID}, $plate, $protein1, @shuffled_vals);
    $csv_ser->print($SER_TEST, \@fields) or die "Failed to write row: " . $csv_ser->error_diag();
}
close $SER;
close $SER_TEST;
sub protein_col_name {
    my ($n) = @_;
    my $result = '';
    while ($n > 0) {
        $n--;
        my $remainder = $n % 26;
        my $letter = chr(65 + $remainder);
        $result = $letter . $result;
        $n = int($n / 26);
    }
    return $result;
}