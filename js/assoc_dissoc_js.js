function assoc_dissoc_js (o,remove_key,add_key,add_value)
{
var assoc_js = function assoc_js(o, k, v)
{
  o[k]=v;
  return o;
};
var dissoc_js = function dissoc_js(obj,k)
{
  delete obj[k];
  return obj;
}
  o = dissoc_js(o,remove_key);
  o = assoc_js(o,add_key,add_value);
  return o;
}