"""Module to house functions for an LLM agent to use."""
import logging

import arxiv_articles
import pandas as pd
import summarize_text

from hamilton import base, driver

logger = logging.getLogger(__name__)


def get_articles(query: str) -> pd.DataFrame:
    """Use this function to get academic papers from arXiv to answer user questions.

    :param query: User query in JSON. Responses should be summarized and should include the article URL reference
    :return: List of dictionaries with title, summary, article_url, pdf_url
    """
    dr = driver.Driver({}, arxiv_articles, adapter=base.SimplePythonGraphAdapter(base.DictResult()))
    inputs = {
        "embedding_model_name": "text-embedding-ada-002",
        "max_arxiv_results": 5,
        "article_query": query,
        "max_num_concurrent_requests": 5,
        "data_dir": "./data",
        "library_file_path": "./data/arxiv_library.csv",
    }
    dr.display_all_functions("./get_articles", {"format": "png"})
    result = dr.execute(["arxiv_result_df", "save_arxiv_result_df"], inputs=inputs)
    logger.info(f"Added {result['save_arxiv_result_df']} to our DB.")
    _df = result["arxiv_result_df"]
    # _df = pd.read_csv(inputs["library_file_path"])
    return _df[["title", "summary", "article_url", "pdf_url"]].to_dict(orient="records")


def read_article_and_summarize(query: str) -> str:
    """Use this function to read whole papers and provide a summary for users.

    You should NEVER call this function before get_articles has been called in the conversation.

    :param query: Description of the article in plain text based on the user's query.
    :return: Summarized text of the article given the query.
    """
    dr = driver.Driver({}, summarize_text, adapter=base.SimplePythonGraphAdapter(base.DictResult()))
    inputs = {
        "embedding_model_name": "text-embedding-ada-002",
        "openai_gpt_model": "gpt-3.5-turbo-0613",
        "user_query": query,
        "top_n": 1,
        "max_token_length": 1500,
        "library_file_path": "./data/arxiv_library.csv",
    }
    dr.display_all_functions("./read_article_and_summarize", {"format": "png"})
    result = dr.execute(["summarize_text"], inputs=inputs)
    return result["summarize_text"]


if __name__ == "__main__":
    """Code to quickly integration test."""
    from hamilton import log_setup

    log_setup.setup_logging(log_level=log_setup.LOG_LEVELS["DEBUG"])
    _df = get_articles("ppo reinforcement learning")
    print(_df)
    _summary = read_article_and_summarize("PPO reinforcement learning sequence generation")
    print(_summary)
