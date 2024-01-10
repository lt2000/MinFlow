# Set Miniconda version and Python version
PYTHON_VERSION="3.9"

# Download Miniconda installer
wget https://repo.anaconda.com/archive/Anaconda3-2022.05-Linux-x86_64.sh -O anaconda.sh

# Install Miniconda
bash anaconda.sh -b -p $HOME/anaconda3
rm anaconda.sh

# Initialize conda and update
conda init
source ~/.zshrc

# Create a new Python environment
conda create --yes --name minflow python=$PYTHON_VERSION
conda activate minflow

# Display conda info
conda info

echo "Anaconda and Python $PYTHON_VERSION have been successfully installed. You are now in a new conda environment named 'minflow'."
