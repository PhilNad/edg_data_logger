# EDG Data Logger ROS Package (Service/Python)

## Objective
The main objective of this ROS package is to make it convenient for the user to gather a great number of measurements by generating a CSV file with an arbitrary number of measures.

## How does it work?
As this package is exposed to the ROS system as a service, a request made by the user will trigger the service. This request simply consists in a boolean value where a TRUE signifies that the logging should be started and a FALSE signifies that the logging should be stopped. As a response, the service replies with the path to the generated CSV file.

**IMPORTANT NOTE** : The CSV files are, by design, generated within the temporary `/tmp/` directory which gets emptied on every reboot of the machine. As data files can become quite large, this is a kind of protection. But that means that meaningful generated CSV files should be moved somewhere else once the experiment is done.

This service is convenient because of its great flexibility. Indeed, any steam of data expose through ROS topics can be logged easily using this service. To specify which topics should be logged, the user must simply make a list in the configuration file under /edg_data_logger/config/TopicsList.txt

The list of "watched" ROS topics is read every time the service is started. An important limitation to keep in mind is the fact that a row of data will be written in the CSV file at the lowest frequency of all the implicated ROS topics. For example, if there is a new data point on topic #1 every milliseconds and there is a new data point on topic #2 every second, then a new row will be appended to the CSV file every second. That also means that 999/1000 of the data points related with topic #1 wont be logged. This is a design decision but an alternative way of logging data could be explored.

This package is a very interesting example of how a request can be treated by a service independantly of the topic or even of the message type being used. The quite abstract [AnyMsg](http://docs.ros.org/api/rospy/html/rospy.msg.AnyMsg-class.html) is used to this end.
