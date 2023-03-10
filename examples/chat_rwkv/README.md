# ChatRWKV on SkyPilot

Inspired by this [reddit post](https://old.reddit.com/r/MachineLearning/comments/11nre6t/p_rwkv_14b_is_a_strong_chatbot_despite_only/), I tried out this [frontend](https://github.com/BlinkDL/ChatRWKV) to [ChatRWKV](https://github.com/BlinkDL/ChatRWKV). The results are the most impressive I've seen from any self-hosted chatbot.

![ChatRWKV](https://i.imgur.com/jveEuXg.png)

 * On a GCP V100, I'd say it runs faster than chatGPT in generating responses.
 * It's generally more coherent than flexgen
 * It can generate code snippets and long answers.
![airtravel.png](https://i.imgur.com/MfGG1fL.png)
 * It can be quite wrong when it comes to facts.
![fact1.png](https://i.imgur.com/ACk8IYt.png)

# Usage
```
sky launch -c rwkv rwkv.yaml
ssh -L 8000:localhost:8000 llm
# Open http://localhost:8000 in your browser to chat
sky down llm
```