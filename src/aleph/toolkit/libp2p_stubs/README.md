# Copies of files from libp2p

The files under this directory/module are all copied directly from [py-libp2p](https://github.com/libp2p/py-libp2p).
py-libp2p is unmaintained at the moment, and its dependency tree makes it (nearly) uninstallable.
As we only require a small set of files from libp2p (basically, the peer ID classes and the public/private key classes),
we are working around this issue by integrating the code we need.

The libp2p license (dual license Apache + MIT) applies for these files:
* libp2p Apache: https://github.com/libp2p/py-libp2p/blob/master/LICENSE-APACHE
* libp2p MIT: https://github.com/libp2p/py-libp2p/blob/master/LICENSE-MIT

Do not modify these files manually.