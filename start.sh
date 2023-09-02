#!/bin/bash

SERVICE_PID=`ps -ef | grep Rserve | grep -v grep | awk '{print $2}'`

echo "$SERVICE_PID"

if [ "$SERVICE_PID" != "" ]; then 
	kill -9 $SERVICE_PID
	sleep 2
	echo "Rserve has been killed!"
	/usr/R/bin/R CMD Rserve --RS-enable-remote --no-save
	echo "Rserve has been started!"
else 
	/usr/R/bin/R CMD Rserve --RS-enable-remote --no-save
	echo "Rserve started successfully!"
fi
