import random

def Subset(backends, client_id, subset_size):
  subset_count = len(backends) // subset_size

  # Group clients into rounds; each round uses the same shuffled list:
  round = client_id / subset_count
  random.seed(round)
  random.shuffle(backends)

  # The subset id corresponding to the current client:
  subset_id = client_id % subset_count

  start = subset_id * subset_size
  return backends[start: start+subset_size]


if __name__ == "__main__":
    backends = [0, 1]
    clients = [0, 1, 2, 3]

    for client in clients:
        subset = Subset(backends, client, 1)
        print(f"client: {client} --> subset: {subset}")