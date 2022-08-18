# Databricks notebook source
myname = dbutils.widgets.get("myname")
print(f"this is {myname}'s job1")
