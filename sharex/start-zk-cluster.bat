@echo off
set CP=C:\zookeeper\apache-zookeeper-3.8.6-bin\lib\*;C:\zookeeper\apache-zookeeper-3.8.6-bin\conf
set CLASS=org.apache.zookeeper.server.quorum.QuorumPeerMain
set DIR=c:\Users\Samuduni\OneDrive - Sri Lanka Institute of Information Technology\Desktop\DS assignment\ShareX-Dev\sharex

echo Stopping any existing Java processes...
taskkill /f /im java.exe 2>nul
timeout /t 2 /nobreak >nul

echo Starting ZooKeeper Ensemble...
start /b java -cp "%CP%" %CLASS% "%DIR%\zk-cluster\zoo1.cfg" > node1.out 2> node1.err
start /b java -cp "%CP%" %CLASS% "%DIR%\zk-cluster\zoo2.cfg" > node2.out 2> node2.err
start /b java -cp "%CP%" %CLASS% "%DIR%\zk-cluster\zoo3.cfg" > node3.out 2> node3.err

echo ZooKeeper Ensemble Started in background!
timeout /t 20 /nobreak >nul
