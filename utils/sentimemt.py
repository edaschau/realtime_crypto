import logging
from together import Together
from keys.together import together_api_key
import params

NEWS_PROMPTS = params.get_news_prompts()
TWITTER_PROMPTS = params.get_twitter_prompts()

def get_model_responses(message, model, crypto_name, is_twitter=False):
    responses = {}
    
    prompts = TWITTER_PROMPTS if is_twitter else NEWS_PROMPTS
    content_type = "tweet" if is_twitter else "news article"
    
    base_system_prompt = f"""
    You are an expert in analyzing {content_type}s about cryptocurrency and {crypto_name}. 
    You will be given a {content_type} about {crypto_name} and asked to rate a specific aspect.
    Provide a single integer score from 1 to 10 based on the content for the given aspect.
    Only respond with the score, nothing else. Do not include any explanations.
    Remember, only output a single integer from 1 to 10, nothing else.
    """

    client = Together(api_key=together_api_key())
    for prompt in prompts:
        system_prompt = base_system_prompt + f"\n\nAspect to rate:\n{prompt['prompt']}"
        
        try:
            response = client.chat.completions.create(
                model=model['name'],
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": message},
                ],
                **model['params']
            )
            
            score = response.choices[0].message.content.strip()
            print(f"Aspect: {prompt['aspect']}, Score: {score}")
            
            try:
                score_int = int(score)
                if 1 <= score_int <= 10:
                    responses[prompt['aspect']] = score_int
                else:
                    logging.warning(f"Score out of range for {prompt['aspect']}: {score}")
                    responses[prompt['aspect']] = 5  # Default to neutral if out of range
            except ValueError:
                logging.warning(f"Invalid response for {prompt['aspect']}: {score}")
                responses[prompt['aspect']] = 5  # Default to neutral if parsing fails
        
        except Exception as e:
            logging.error(f"Error processing model response for {prompt['aspect']}: {e}")
            responses[prompt['aspect']] = 5  # Default to neutral if processing fails
    
    return responses

