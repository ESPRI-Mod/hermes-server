
# Get log file data.
with open(__file__.replace('.py', '.log'), 'r') as f:
	data = f.readlines()

# Strip line endings.
data = [l.replace('\n', '') for l in data]

# Filter out lines not related to block allocations.
data = [l for l in data if l.find("allocation: ") > -1]

# Extract allocation fields.
data = [l.split("allocation: ")[-1] for l in data]

# Derive sorted unique set.
data = sorted(list(set(data)))

# Write to ooutput file.
with open(__file__.replace('.py', '.csv'), 'w') as f:
	for l in data:
		f.write(l.replace(' :: ', ', '))
		f.write('\n')
