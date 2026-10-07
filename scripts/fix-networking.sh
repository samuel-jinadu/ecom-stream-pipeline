#!/bin/bash

# Codespaces has a container to container networking issue, if you are trying to run this in codespaces and it fails with mysterious errors, run this

sudo iptables-legacy -P FORWARD ACCEPT && sudo iptables-legacy -I FORWARD 1 -i br-+ -j ACCEPT && sudo iptables-legacy -I FORWARD 1 -o br-+ -j ACCEPT || true