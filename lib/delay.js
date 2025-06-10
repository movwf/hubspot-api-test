const delay = delayInMs => new Promise(res => setTimeout(() => res(), delayInMs));

export default delay;