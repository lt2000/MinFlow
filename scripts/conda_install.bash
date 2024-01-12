#!/bin/zsh

# Define the URL and installation directory for the Anaconda installation file
anaconda_installer_url="https://repo.anaconda.com/archive/Anaconda3-2022.05-Linux-x86_64.sh"
install_dir="$HOME/anaconda3"

# Download the Anaconda installation file
echo "Downloading Anaconda installer..."
curl -O $anaconda_installer_url

# Run the Anaconda installation process
echo "Installing Anaconda..."
bash Anaconda3-2022.05-Linux-x86_64.sh -b -p $install_dir

# Add Anaconda to the PATH
echo "Adding Anaconda to PATH..."
echo "export PATH=$HOME/anaconda3/bin:\$PATH" >> $HOME/.zshrc
source $HOME/.zshrc
conda init zsh
source $HOME/.zshrc

# Remove the downloaded installation file
echo "Cleaning up..."
rm Anaconda3-2022.05-Linux-x86_64.sh

echo "Anaconda has been installed successfully. Please restart your shell or run 'source $HOME/.zshrc'."

# Create and activate the conda environment
echo "Creating and activating 'minflow' environment..."
conda create --yes --name minflow python=3.9
source $HOME/.zshrc
conda activate minflow

echo "minflow environment has been created and activated. You can start using it!"
