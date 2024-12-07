#!/bin/bash

# Name of the tmux session
SESSION_NAME="cs171-final-project"

# Ensure PORT is set (fallback to default if not exported)
PORT="${PORT:-9000}"

# Create a new tmux session in detached mode
tmux new-session -d -s "$SESSION_NAME"

# Commands for each server
commands=(
    "make network_server; python3 -u network_server.py $PORT 3"
    "make server0"
    "make server1"
    "make server2"
)

# Send the first command to the first pane
tmux send-keys -t "${SESSION_NAME}:0.0" "${commands[0]}" C-m

# Loop through the remaining commands and create new panes
for i in "${!commands[@]}"; do
  if [ "$i" -eq 0 ]; then
    continue
  fi
  # Split the window horizontally for each new command
  tmux split-window -h -t "$SESSION_NAME:0.$((i-1))"
  tmux send-keys -t "${SESSION_NAME}:0.$i" "${commands[i]}" C-m
done

# Arrange all panes in a tiled layout
tmux select-layout -t "$SESSION_NAME" tiled

# Attach to the tmux session
tmux attach-session -t "$SESSION_NAME"
