package ESSP::Model::MQ;

use strict;
use warnings;
use v5.10;
use utf8;

use Net::RabbitMQ;
use Mojo::JSON qw(decode_json encode_json);

use constant {
    USER => 0,
    PASS => 1,
    HOST => 2,
    PORT => 3,
};
my $mq_url_re = qr/^amqp:\/\/([a-z]+):([^\@]+)@([^:]+):(\d+)$/;

sub new {
    my ($class, $app) = @_;

    my $self = {
        app      => $app,
        mq       => undef,
        channel  => 1,
        exchange => $ENV{MQ_EXCHANGE_NAME} // 'msgio',
        queue    => $ENV{MQ_QUEUE_NAME} // 'essp',
    };
    bless $self, $class;

    $self->init();

    return $self;
}

sub init {
    my $self = shift;

    my $mq;
    eval {
        my $mq_url = $ENV{MQ_URL} // 'amqp://guest:guest@localhost:5672';
        my @fields = $mq_url =~ $mq_url_re;

        $mq = Net::RabbitMQ->new;
        $mq->connect($fields[HOST], {
            user        => $fields[USER],
            password    => $fields[PASS],
            vhost       => "/",
            channel_max => 1024,
        });

        $mq->channel_open($self->{channel});

        $mq->exchange_declare($self->{channel}, $self->{exchange});
        $mq->queue_declare($self->{channel}, $self->{queue}, {
            passive     => 0,
            durable     => 1,
            exclusive   => 0,
            auto_delete => 0
        });

        $mq->queue_bind($self->{channel}, $self->{queue},
            $self->{exchange}, "");
    };

    if ($@ ne "") {
        $self->{app}->log->error("mq: failed to connect: $@");
        exit(1);
    }

    $self->{mq} = $mq;
}

sub publish {
    my ($self, $rec) = @_;

    eval {
        my $msg = encode_json($rec);
        $self->{mq}->publish($self->{channel}, $self->{queue}, $msg);
    };

    if ($@ ne "") {
        $self->{app}->log->error("mq: failed to publish: $@");
        return undef;
    }

    $self->{app}->log->info("mq: published [$rec->{id}]");

    $self->{mq}->confirm_select;
}

1;