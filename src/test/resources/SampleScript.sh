#!/bin/sh
while true;do
  read USERNAME
  if [[ -z $USERNAME ]];then
    break
  else
    echo "Welcome User, $USERNAME."
  fi
done
