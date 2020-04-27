package ESSP::Controller::Acceptor;
use Mojo::Base 'Mojolicious::Controller';
use Mojo::JSON qw(decode_json encode_json);

use UUID::Tiny ':std';

use constant {
    HTTP_NOT_FOUND      => 404,
    HTTP_BAD_REQUEST    => 400,
    HTTP_INTERNAL_ERROR => 500,
    HTTP_ACCEPTED       => 202,
};

use constant RE_EMAIL => qr/^[a-z0-9_.+-]+@[a-z0-9-]+\.[a-z0-9-.]+$/; # very basic email regexp
use constant RE_INT => qr/^\d+$/;
use constant RE_UUID => qr/^\/notifs\/([0-9a-f]{8}\-([0-9a-f]{4}\-){3}[0-9a-f]{12})$/i;

my $default_page_size = $ENV{PAGE_SIZE} // 20;

sub show_item {
    my $self = shift;

    my $id;
    if ($self->req->url =~ RE_UUID) {
        $id = $1;
    }
    else {
        $self->send_error("Invalid ID format", HTTP_BAD_REQUEST);
        return;
    }

    my ($rec, $err) = $self->db->find($id);
    if (defined $err) {
        $self->log->error("db: failed to get record");
        $self->send_error("Failed to get message", HTTP_INTERNAL_ERROR);
    }

    $rec->{to} = [ split(":", $rec->{to}) ];

    $self->render(json => $rec);
}

sub show_list {
    my $self = shift;

    my $page_size = $self->check_param("per_page", $default_page_size);
    my $page = $self->check_param("page", 1);
    my $offset = ($page - 1) * $page_size;

    my ($data, $err) = $self->db->list($page_size, $offset);

    if (defined $err) {
        $self->log->error("db: error getting list: $err");
        return;
    }

    my $max_page = int($data->{count} / $page_size) + 1;

    $self->res->headers->add("X-Total" => $data->{count});
    $self->res->headers->add("X-Total-Pages" => $max_page);
    $self->res->headers->add("X-Per-Page" => $page_size);
    $self->res->headers->add("X-Page" => $page);

    if ($page + 1 <= $max_page) {
        $self->res->headers->add("X-Next-Page" => $page + 1);
    }

    if ($page > 1 && $page <= $max_page) {
        $self->res->headers->add("X-Prev-Page" => $page - 1);
    }

    if (scalar @{$data->{list}} < 1) {
        $self->send_error("Empty list", HTTP_NOT_FOUND);
        return;
    }

    foreach my $rec (@{$data->{list}}) {
        $rec->{to} = [ split(":", $rec->{to}) ];
    }
    $self->render(json => $data->{list});
}


sub accept_item {
    my $self = shift;
    # $c->log->debug($c->req->body);

    return $self->send_error("Wrong request method", HTTP_BAD_REQUEST)
        if $self->req->method() ne "POST";

    my $rec_data;
    eval {$rec_data = decode_json($self->req->body);};
    return $self->send_error("Wrong request format", HTTP_BAD_REQUEST)
        unless defined $rec_data;

    return $self->send_error("Empty email list", HTTP_BAD_REQUEST)
        if scalar @{$rec_data->{to}} < 1;

    foreach my $addr (@{$rec_data->{to}}) {
        return $self->send_error("Invalid email", HTTP_BAD_REQUEST)
            unless $addr =~ RE_EMAIL;
    }

    $rec_data->{id} = create_uuid_as_string(UUID_V4);

    unless ($self->mq->publish($rec_data)) {
        $self->send_error("Error sending message", HTTP_INTERNAL_ERROR);
    }

    $self->render(json => { id => $rec_data->{id} }, status => HTTP_ACCEPTED);
}

sub check_param {
    my ($self, $field, $default) = @_;

    my $value = $self->req->params->param($field) // "";
    if ($value !~ RE_INT) {
        $value = $default;
    }

    return $value;
}

sub send_error {
    my ($self, $text, $status) = @_;

    $self->log->error("acceptor: [$text]");
    $self->render(text => $text, status => $status);
}

1;
