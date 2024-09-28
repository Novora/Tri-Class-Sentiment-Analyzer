# Research for TCSA

1. Use BiLSTM for architecture with pytorch.
2. Configure parameters in a `config.toml` file.
3. Checkpointing of the model weights, and also the metrics, with MLflow. (Checkpoints model metrics and weights)
4. Early Stopping: Stop when we dont see much improvement.
5. Along with the training run metrics and weights, we need to store hyperparameters(batch size, learning rate, epocs).
6. Each Epoch is a subcheckpoint.
7. Append 5-character commit hash to the training run data, so we keep track of model architecture.
8. Copy the `config.toml` into the training run labeled folder.
9. Set random seeds and record them.
10. Detailed logging.