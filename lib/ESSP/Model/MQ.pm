package ESSP::Model::MQ;

use strict;
use warnings;
use v5.10;
use utf8;

# use Net::RabbitMQ;
use Net::AMQP::RabbitMQ::PP;
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
        delivery_tag => 0,
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

        $mq = Net::AMQP::RabbitMQ::PP->new;
        $mq->connect(
            host        => $fields[HOST],
            user        => $fields[USER],
            password    => $fields[PASS],
            vhost       => "/",
            channel_max => 1024,
        );

        $mq->channel_open(channel => $self->{channel});

        # $mq->exchange_declare($self->{channel}, $self->{exchange});
        # $mq->exchange_declare(
        #     channel       => $self->{channel},
        #     exchange      => $self->{exchange},
        #     exchange_type => 'direct',
        #     auto_delete   => 1,
        # );


        # $mq->queue_declare($self->{channel}, $self->{queue}, {
        #     passive     => 0,
        #     durable     => 1,
        #     exclusive   => 0,
        #     auto_delete => 0
        # });

        $mq->queue_declare(
            channel     => $self->{channel},
            queue       => $self->{queue},
            passive     => 0,
            durable     => 1,
            exclusive   => 0,
            auto_delete => 0,
        );

        # $mq->queue_bind($self->{channel}, $self->{queue},
        #     $self->{exchange}, "");

        $mq->queue_bind(
            channel     => $self->{channel},
            queue       => $self->{queue},
            exchange    => $self->{exchange},
            routing_key => $self->{queue},
        );

        my $res = $mq->confirm_select(channel => $self->{channel});

        use Data::Dumper;
        $self->{app}->log->info(Dumper($res));

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
        $self->{mq}->basic_publish(
            channel  => $self->{channel},
            queue    => $self->{queue},
            payload  => $msg,
            exchange => $self->{exchange},
            props    => {
                content_type => "application/json",
            }
        );
    };

    if ($@ ne "") {
        $self->{app}->log->error("mq: failed to publish: $@");
        return undef;
    }

    $self->{app}->log->info("mq: published [$rec->{id}]");
    $self->{delivery_tag}++;

    # waiting for confirmation
    my $delivery_tag;
    eval{
        $self->{mq}->basic_consume(
            channel  => $self->{channel},
            queue    => $self->{queue},
            exchange => $self->{exchange},
        );

        $delivery_tag = $self->{mq}->receive->{method_frame}{delivery_tag};
    };

    if($@ ne ""){
        $self->{app}->log->error("mq: confirmation failed");
        return undef;
    }

    if($delivery_tag != $self->{delivery_tag}){
        $self->{app}->log->error("mq: confirmation count mismatch");
        return undef;
    }

    return 1;
}

1;