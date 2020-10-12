# Tree Schema / Faust Integration

This example shows how to use Faust Models to build your Tree Schema schemas, fields and transformations.

# Requirements

### Kafka
You will need to have Kafka running locally with the bootstrap server exposed on it's native port 9092. If you're using a Mac you can quickly get it up and running with brew:

```bash
brew install kafka
brew services start zookeeper
brew services start kafka
```

And if you want to turn it off when you're done
```
brew services stop kafka
brew services stop zookeeper
```

### Python
You will need to have additional requirements installed that are not included with the Tree Schema Python Client, you can install all of the requirements with:

```bash 
pip install treeschema faust
```

# To Run

There are three steps to run this complete application, each of the commands below should be run in a different terminal from this directory.

1. Send events to Kafka that will be used as input into the applicaiton

```bash 
python event_generator.py
```

2. Start the Faust App - this will read events from Kafka, update your metadata in Tree Schema and send output events back to Kafka

```bash 
faust -A app worker -l info 
```

3. Start the output consumer to read the output messages

```bash 
python read_output.py
```

You should see some output that looks like this:

```bash
{'user_id': 'user_7', 'user_cnt': 7, 'user_max': 0.947803769568057, 'user_min': 0.2697048183379196, '__faust': {'ns': 'app.UserOutput'}}
{'user_id': 'user_5', 'user_cnt': 3, 'user_max': 0.7828307003967205, 'user_min': 0.15740027633670461, '__faust': {'ns': 'app.UserOutput'}}
{'user_id': 'user_3', 'user_cnt': 3, 'user_max': 0.6893702606783305, 'user_min': 0.002440115927178632, '__faust': {'ns': 'app.UserOutput'}}
{'user_id': 'user_7', 'user_cnt': 8, 'user_max': 0.947803769568057, 'user_min': 0.2697048183379196, '__faust': {'ns': 'app.UserOutput'}}
{'user_id': 'user_8', 'user_cnt': 2, 'user_max': 0.4544665072165749, 'user_min': 0.2637137247615674, '__faust': {'ns': 'app.UserOutput'}}
```
