import streamlit as st

st.title("Simple Streamlit App")

name = st.text_input("Enter your name")
st.write(f"Hello, {name}!")

x = st.slider("Select a number", 0, 100, 25)
st.write(f"Square of the number is: {x * x}")
