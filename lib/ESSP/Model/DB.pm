package ESSP::Model::DB;
# use Mojo::Base -base;
use strict;
use warnings FATAL => 'all';

use constant QUERY_LIST => 'select * from records order by created_at, id asc limit ? offset ?';
use constant QUERY_COUNT => 'select count(*) from records';
use constant QUERY_GET => 'select * from records where id = ?';

sub new {
    my ($class, $app) = @_;

    my $self = {
        db  => undef,
        app => $app,
    };

    my $pg;
    my $pg_url = $ENV{PG_URL} // 'postgresql://msgio:msgio-ess@dev00.caboom.net:5432/msgio-ess';

    eval {
        $pg = Mojo::Pg->new($pg_url);
    };

    if ($@ ne "") {
        $app->log->error("[f] db: failed to connect, stop: " . $@);
        exit(2);
    }

    unless ($pg->db->ping()) {
        $app->log->error("[f] db: ping failed, stop");
        exit(2);
    }

    $self->{db} = $pg->db;

    bless $self, $class;
}

sub list {
    my ($self, $limit, $offset) = @_;
    my $res;

    $self->{app}->log->info("limit: " . $limit, "offset: " . $offset);

    eval {
        $res = $self->{db}->query(QUERY_LIST, $limit, $offset);
    };

    if ($@ ne "") {
        return undef, $@;
    }

    my $result = {
        list => $res->hashes->to_array,
    };

    eval {
        $res = $self->{db}->query(QUERY_COUNT);
    };

    if ($@ ne "") {
        return undef, $@;
    }

    $result->{count} = $res->array()->[0];

    $self->{app}->log->info("count: ", $result->{count});
    return $result;
}

sub find {
    my ($self, $id) = @_;
    my $rec;
    eval {
        my $res = $self->{db}->query(QUERY_GET, $id);
        $rec = $res->hash;
        $res->finish;
    };
    if ($@ ne "") {
        return undef, $@;
    }
    return $rec;
}

1;
