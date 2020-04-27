package ESSP;
use Mojo::Base 'Mojolicious';

use ESSP::Model::DB;
use ESSP::Model::MQ;
use Mojo::Pg;

sub startup {
  my $self = shift;

  $self->plugin('Config');
  $self->secrets($self->config('secrets'));

  $self->helper(db => sub { state $db = ESSP::Model::DB->new($self) });
  $self->helper(mq => sub { state $mq = ESSP::Model::MQ->new($self) });

  my $r = $self->routes;
  $r->get('/' => sub { shift->redirect_to('notifs') });

  $r->post('/notifs')->to('acceptor#accept_item')->name('accept_item');
  $r->get('/notifs/:id')->to('acceptor#show_item')->name('show_item');
  $r->get('/notifs/')->to('acceptor#show_list')->name('show_list');
}

1;
