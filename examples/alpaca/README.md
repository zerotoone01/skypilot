# Finetuning the LlaMA model with Stanford Alpaca on any cloud

This example shows how to finetune the LlaMA model with Stanford Alpaca on any cloud in a single click. The following command is all you need to run the example (please check the TODOs in [finetune.yaml](finetune.yaml) and replace those buckets with your own):

```bash
sky launch -c alpaca --env WANDB_MODE=offline finetune.yaml
# If you have WANDB_API_KEY set locally, you can use the following command instead:
sky launch -c alpaca --env WANDB_API_KEY finetune.yaml
```
