import torch


if __name__ == "__main__":
    assert torch.cuda.is_available(), "CUDA is not available."