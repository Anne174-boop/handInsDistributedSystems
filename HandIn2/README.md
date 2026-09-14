a) What are packages in your implementation? What data structure do you use to transmit data and meta-data?
    The packages in our implementation are the messages. We chose to use a struct to create a type message, thus allowing us to send both the seq AND the ack back and forth between server and client.
    We use the data structure message to transmit the meta-data from seq and ack

b) Does your implementation use threads or processes? Why is it not realistic to use threads?
    We use threads (goroutines for both server and client).
    It is not realistic to use threads when we run across a network. It works on a smaller scale, but when using threads in larger scales they are too heavy and slow and therefor as programmers we should call for different implementations, such as processes.

c) In case the network changes the order in which messages are delivered, how would you handle message re-ordering?
    We would choose to handle message re-ordering by using sequence numbers for each package, when transmitting between server and client, that would ensure that the program still runs the correct order, despite message re-ordering

d) In case messages can be delayed or lost, how does your implementation handle message loss?
    We chose to implement solution [1] and therefor we did not handle messages that are losy or delayed, but what we could have done is acknowledge lost or delayed messages. If they are delayed, we handle it by implementing some sort of queue (or buffer) that ensures that the rest of the package await the delayed message. If the message is lost we acknowledge the loss and then try to resend it. We need to use the sequence numbers for the package (described in exercise c) for the program to know if something is lost or delayed.

e) Why is the 3-way handshake important?
    The 3-way handshake is important to ensure a more reliable connectivity between a server and a client -> hence being connection-oriented.
    The 3-way handshake uses three 3 steps:
        1. SYN (client propose a starting sequence value)
        2. SYN-ACK (server now acknowledge the SYN of the server and is incremented by 1)
        3. ACK (at last the client acknowledge the servers SYN and increment the sequence by 1)
    We showed the implementation of these three steps in the code, and the importance of the 3-way handshake relies on these three steps before any data transfer occurs. The model synchronizes the sequence numbers and give us a guarentee that both the server and client are ready to transmit data.
