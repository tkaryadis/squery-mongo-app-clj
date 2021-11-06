function assoc_dissoc_js (o,remove_key,add_key,add_value)
{
  o = dissoc_js(o,remove_key);
  o = assoc_js(o,add_key,add_value);
  return o;
}